using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Discovers the user-defined types declared in a keyspace and registers a
/// <see cref="UdtMap"/> for each one on the supplied <see cref="ISession"/>.
///
/// The DataStax C# Cassandra driver decodes a UDT-typed column only when a
/// <see cref="UdtMap"/> has been registered against the session for that UDT;
/// otherwise the cell is surfaced as a raw <c>byte[]</c> and cannot be bound
/// back into a prepared statement parameter on another session, which causes
/// a protocol-level serialization failure on write.
///
/// Because target schemas are not known at compile time, a backing CLR type
/// is generated at runtime for each UDT shape (one property per field, using
/// CLR types compatible with the driver's automap). The same generated type
/// is reused for the matching UDT on both the source and target sessions so
/// that values read from the source can be bound on the target without any
/// intermediate conversion.
/// </summary>
internal static class DynamicUdtRegistrar
{
    private static readonly object _moduleLock = new();
    private static ModuleBuilder? _module;
    private static readonly ConcurrentDictionary<string, Type> _typeCache = new(StringComparer.OrdinalIgnoreCase);

    private static ModuleBuilder GetModule()
    {
        lock (_moduleLock)
        {
            if (_module != null) return _module;
            var asmName = new AssemblyName("CassandraMigration.DynamicUdts");
            var asm = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
            _module = asm.DefineDynamicModule("DynamicUdtsModule");
            return _module;
        }
    }

    /// <summary>
    /// Registers a <see cref="UdtMap"/> on <paramref name="session"/> for every
    /// user-defined type in <paramref name="keyspace"/>. UDTs are processed in
    /// dependency order so that nested UDT references resolve to previously
    /// generated CLR types. Safe to call repeatedly on the same session.
    /// </summary>
    public static async Task RegisterAsync(ISession session, string keyspace,
        IReadOnlyList<SchemaManager.UserDefinedTypeDef>? udts = null)
    {
        MigrationUtilities.ValidateCqlIdentifier(keyspace);

        udts ??= await SchemaManager.GetUserDefinedTypesAsync(session, keyspace);
        if (udts.Count == 0) return;

        var sorted = SchemaManager.TopologicallySortUdts(udts.ToList());
        var known = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

        foreach (var udt in sorted)
        {
            var clrType = GetOrCreateClrType(keyspace, udt, known);
            known[udt.TypeName] = clrType;
            try
            {
                var map = (UdtMap)typeof(UdtMap)
                    .GetMethod(nameof(UdtMap.For))!
                    .MakeGenericMethod(clrType)
                    .Invoke(null, new object?[] { udt.TypeName, keyspace })!;
                session.UserDefinedTypes.Define(map);
            }
            catch (ArgumentException)
            {
                // The driver throws ArgumentException with a "already added"
                // message when the same (keyspace, typeName) pair is defined
                // twice on a session. Idempotent re-registration is intended
                // (source + target registrars can both touch the same map),
                // so this specific case is benign.
            }
            // NOTE: we deliberately do NOT catch InvalidOperationException
            // here. The driver raises IOE for real misconfigurations — e.g.
            // a CLR type that does not match the on-server UDT shape — and
            // swallowing it would let column binds silently produce wrong
            // data downstream. Let it propagate so the job fails fast at
            // setup rather than corrupting rows mid-migration.
        }
    }

    private static Type GetOrCreateClrType(string keyspace,
        SchemaManager.UserDefinedTypeDef udt,
        IReadOnlyDictionary<string, Type> known)
    {
        // Cache key includes the field signature so two UDTs that share a
        // name across keyspaces (or across runs with altered shapes) do not
        // collide. The same generated type must be reused on the source and
        // target sessions for cross-session bind to succeed.
        var sig = udt.TypeName + "|" + string.Join(",",
            udt.FieldNames.Zip(udt.FieldTypes, (n, t) => n + ":" + t));
        return _typeCache.GetOrAdd(sig, _ => BuildClrType(keyspace, udt, known));
    }

    private static Type BuildClrType(string keyspace,
        SchemaManager.UserDefinedTypeDef udt,
        IReadOnlyDictionary<string, Type> known)
    {
        var module = GetModule();
        var safeName = "Udt_" + Sanitize(keyspace) + "_" + Sanitize(udt.TypeName);
        var typeBuilder = module.DefineType(
            safeName,
            TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.AutoClass
            | TypeAttributes.AnsiClass | TypeAttributes.BeforeFieldInit,
            typeof(object));

        typeBuilder.DefineDefaultConstructor(
            MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName
            | MethodAttributes.RTSpecialName);

        for (int i = 0; i < udt.FieldNames.Count; i++)
        {
            var clrType = MapCqlTypeToClr(udt.FieldTypes[i], known);
            DefineAutoProperty(typeBuilder, udt.FieldNames[i], clrType);
        }

        return typeBuilder.CreateType()!;
    }

    private static void DefineAutoProperty(TypeBuilder tb, string name, Type type)
    {
        var field = tb.DefineField("_" + name, type, FieldAttributes.Private);
        var prop = tb.DefineProperty(name, PropertyAttributes.HasDefault, type, null);

        var getterAttrs = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig;
        var getter = tb.DefineMethod("get_" + name, getterAttrs, type, Type.EmptyTypes);
        var gIl = getter.GetILGenerator();
        gIl.Emit(OpCodes.Ldarg_0);
        gIl.Emit(OpCodes.Ldfld, field);
        gIl.Emit(OpCodes.Ret);

        var setter = tb.DefineMethod("set_" + name, getterAttrs, null, new[] { type });
        var sIl = setter.GetILGenerator();
        sIl.Emit(OpCodes.Ldarg_0);
        sIl.Emit(OpCodes.Ldarg_1);
        sIl.Emit(OpCodes.Stfld, field);
        sIl.Emit(OpCodes.Ret);

        prop.SetGetMethod(getter);
        prop.SetSetMethod(setter);
    }

    private static string Sanitize(string s)
        => new(s.Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());

    /// <summary>
    /// Maps a CQL type string (as exposed by
    /// <c>system_schema.types.field_types</c>) to the CLR type the driver
    /// expects for a UDT field. Unknown types fall back to
    /// <see cref="object"/>, which the driver accepts for collection-of-UDT
    /// values that are bound back positionally.
    /// </summary>
    private static Type MapCqlTypeToClr(string cqlType, IReadOnlyDictionary<string, Type> known)
    {
        var t = cqlType.Trim();

        if (t.StartsWith("frozen<", StringComparison.OrdinalIgnoreCase) && t.EndsWith(">"))
            t = t.Substring(7, t.Length - 8).Trim();

        // A nested UDT must bind to the exact generated CLR type so that
        // cross-session round-trip works.
        if (known.TryGetValue(t, out var nested)) return nested;

        if (StartsWithCi(t, "list<") || StartsWithCi(t, "set<") || StartsWithCi(t, "map<"))
        {
            return typeof(object);
        }

        // tuple<...> requires a strongly-typed System.Tuple<T1,T2,...>; the
        // driver's UDT field mapper rejects `object` for tuple fields with
        // "No converter is available from System.Object to System.Tuple`N".
        if (StartsWithCi(t, "tuple<") && t.EndsWith(">"))
        {
            var inner = t.Substring(6, t.Length - 7);
            var parts = SplitTopLevel(inner);
            var argTypes = parts.Select(p => MapCqlTypeToClr(p, known)).ToArray();
            return BuildSystemTupleType(argTypes);
        }

        return t.ToLowerInvariant() switch
        {
            "ascii" or "text" or "varchar" => typeof(string),
            "inet" => typeof(System.Net.IPAddress),
            "bigint" or "counter" => typeof(long),
            "time" => typeof(LocalTime),
            "blob" => typeof(byte[]),
            "boolean" => typeof(bool),
            "date" => typeof(LocalDate),
            "decimal" => typeof(decimal),
            "double" => typeof(double),
            "duration" => typeof(Duration),
            "float" => typeof(float),
            "int" => typeof(int),
            "smallint" => typeof(short),
            "timestamp" => typeof(DateTimeOffset),
            "timeuuid" or "uuid" => typeof(Guid),
            "tinyint" => typeof(sbyte),
            "varint" => typeof(System.Numerics.BigInteger),
            _ => typeof(object)
        };
    }

    private static bool StartsWithCi(string s, string prefix)
        => s.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Splits a comma-separated CQL type list at top-level commas,
    /// respecting angle-bracket nesting (e.g. <c>tuple&lt;text,int&gt;,boolean</c>
    /// splits into two parts, not three).
    /// </summary>
    private static List<string> SplitTopLevel(string s)
    {
        var parts = new List<string>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.Length; i++)
        {
            var c = s[i];
            if (c == '<') depth++;
            else if (c == '>') depth--;
            else if (c == ',' && depth == 0)
            {
                parts.Add(s.Substring(start, i - start).Trim());
                start = i + 1;
            }
        }
        if (start < s.Length) parts.Add(s.Substring(start).Trim());
        return parts;
    }

    /// <summary>
    /// Builds a closed <c>System.Tuple&lt;...&gt;</c> type from the supplied
    /// element types. Arity 1-7 maps directly; arity 8+ uses TRest nesting.
    /// </summary>
    private static Type BuildSystemTupleType(Type[] args)
    {
        if (args.Length == 0) return typeof(object);
        if (args.Length <= 7)
        {
            var open = args.Length switch
            {
                1 => typeof(Tuple<>),
                2 => typeof(Tuple<,>),
                3 => typeof(Tuple<,,>),
                4 => typeof(Tuple<,,,>),
                5 => typeof(Tuple<,,,,>),
                6 => typeof(Tuple<,,,,,>),
                7 => typeof(Tuple<,,,,,,>),
                _ => throw new InvalidOperationException()
            };
            return open.MakeGenericType(args);
        }
        var head = args.Take(7).ToArray();
        var rest = BuildSystemTupleType(args.Skip(7).ToArray());
        return typeof(Tuple<,,,,,,,>).MakeGenericType(head.Concat(new[] { rest }).ToArray());
    }
}
