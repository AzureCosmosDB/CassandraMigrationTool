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
/// At session-init time the C# Cassandra driver decodes a UDT-typed column
/// only if a <see cref="UdtMap"/> is registered for it; otherwise the cell
/// is surfaced as raw <c>byte[]</c> and any attempt to bind it back into a
/// prepared statement on a different session produces a protocol-level
/// serialization failure (matching the customer-reported MarshalException
/// when copying rows that contain UDTs).
///
/// Because the migration tool does not know the customer's UDT shapes at
/// compile time, this helper generates a concrete .NET class per UDT at
/// runtime (one property per UDT field, with a CLR type compatible with
/// the driver's automap), registers a <see cref="UdtMap"/> for that class,
/// and reuses the same generated type for the matching UDT on every
/// session so that a value read from the source can be written back to
/// the target without manual conversion.
///
/// <para><b>Why <see cref="System.Reflection.Emit"/> and not a simpler
/// approach?</b> The DataStax Java/Scala drivers expose an untyped
/// <c>UdtValue</c> wrapper that other migration tools (datastax CDM,
/// scylla-migrator) use to round-trip UDT cells without per-UDT classes.
/// The C# driver (CassandraCSharpDriver 3.21.0) has no equivalent public
/// API:
/// <list type="bullet">
///   <item><description><see cref="UdtMap{T}"/> is generic with a
///   <c>where T : new()</c> constraint and its <c>Automap</c>/<c>ToObject</c>
///   path calls <c>NetType.GetProperty(field.Name)</c>, requiring a real
///   CLR class with one property per UDT field.</description></item>
///   <item><description>The internal <c>UdtSerializer</c> indexes
///   registrations by <c>typeof(T)</c> on the write path, so two distinct
///   UDTs cannot share a single backing class — the second registration
///   would overwrite the first and break writes.</description></item>
///   <item><description><c>UdtMap.ToObject</c> is <c>internal</c>, so a
///   subclass cannot replace the property-mapping flow.</description></item>
///   <item><description>Custom <c>TypeSerializer</c>s registered via
///   <c>Builder.WithTypeSerializers</c> cannot intercept UDT cells — the
///   <c>UdtSerializer</c> path is hard-coded for <c>ColumnTypeCode.Udt</c>.
///   </description></item>
///   <item><description>Binding a raw <c>byte[]</c> to a UDT-typed parameter
///   returns null from <c>UdtSerializer.Serialize</c> (no map for
///   <c>typeof(byte[])</c>), so a bytes-passthrough strategy fails on the
///   write side.</description></item>
/// </list>
/// Generating one CLR class per UDT shape at runtime is therefore the only
/// path through the driver's public API. Roslyn / Castle DynamicProxy /
/// other code-gen libraries all emit IL underneath, so they would add a
/// heavier dependency without removing the underlying technique.
/// TODO: revisit if upstream issue
/// https://github.com/datastax/csharp-driver adds a non-generic
/// <c>UdtMap.For(Type, ...)</c> factory or an <c>IDictionary</c>-backed UDT
/// mapping mode — at that point this entire file can collapse to ~30 lines.
/// </para>
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
    /// Discover every UDT in <paramref name="keyspace"/> on the source
    /// session and register a generated CLR mapping on
    /// <paramref name="session"/> so the driver round-trips UDT-typed
    /// cells through real instances rather than raw bytes.
    /// Idempotent — duplicate registrations are silently swallowed.
    /// </summary>
    public static async Task RegisterAsync(ISession session, string keyspace,
        IReadOnlyList<SchemaManager.UserDefinedTypeDef>? udts = null)
    {
        MigrationUtilities.ValidateCqlIdentifier(keyspace);

        udts ??= await SchemaManager.GetUserDefinedTypesAsync(session, keyspace);
        if (udts.Count == 0) return;

        // Process in dependency order so when we generate a CLR type for a
        // UDT whose fields reference other UDTs, those nested UDT types are
        // already in `known` and we can wire their CLR Type into the
        // generated property — required so the driver decodes nested UDT
        // cells into the same CLR class on read and binds them on write.
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
                // Already registered for this session — fine.
            }
            catch (InvalidOperationException)
            {
                // Same — driver throws if the same UdtMap is defined twice.
            }
        }
    }

    private static Type GetOrCreateClrType(string keyspace,
        SchemaManager.UserDefinedTypeDef udt,
        IReadOnlyDictionary<string, Type> known)
    {
        // Cache by UDT name + field signature so that the same generated CLR
        // type is reused across the source and target sessions (they share a
        // shape after ReplicateUserDefinedTypesAsync), which is required for
        // a row read on the source to be bindable on the target.
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
    {
        var chars = s.Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray();
        return new string(chars);
    }

    /// <summary>
    /// Best-effort mapping from a CQL type string (as exposed by
    /// <c>system_schema.types.field_types</c>) to the CLR type the
    /// driver hands back when it decodes that column. The return type
    /// is intentionally loose for collection-of-UDT cases — the driver
    /// happily binds an <see cref="object"/> property as long as the
    /// runtime value matches what it wrote during decode.
    /// </summary>
    private static Type MapCqlTypeToClr(string cqlType, IReadOnlyDictionary<string, Type> known)
    {
        var t = cqlType.Trim();

        // Unwrap frozen<...>
        if (t.StartsWith("frozen<", StringComparison.OrdinalIgnoreCase) && t.EndsWith(">"))
            t = t.Substring(7, t.Length - 8).Trim();

        // Nested UDT — bind the actual generated CLR type so the driver
        // decodes the nested UDT cell into the same class on read and can
        // re-bind it on write.
        if (known.TryGetValue(t, out var nested)) return nested;

        // Collections — let the driver decide concrete element types
        // by accepting a loose CLR shape. Driver round-trip works because
        // the property is read back as `object` and bound positionally.
        if (StartsWithCi(t, "list<") || StartsWithCi(t, "set<") || StartsWithCi(t, "map<")
            || StartsWithCi(t, "tuple<"))
        {
            return typeof(object);
        }

        return t.ToLowerInvariant() switch
        {
            "ascii" or "text" or "varchar" or "inet" => typeof(string),
            "bigint" or "counter" or "time" => typeof(long),
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
            // Nested user-defined or unknown — driver decodes as object/UdtValue.
            _ => typeof(object)
        };
    }

    private static bool StartsWithCi(string s, string prefix)
        => s.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);
}
