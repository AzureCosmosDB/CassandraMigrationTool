using Newtonsoft.Json;
using CassandraMigrationProcessor.Persistence;

namespace CassandraMigrationProcessor.Context;

/// <summary>
/// Thin convenience wrapper over the shared <see cref="IDocumentStorage"/>
/// (resolved via <see cref="MigrationJobContext.Instance"/>) that bundles
/// JSON (de)serialization with the underlying read/write call.
///
/// "read-then-deserialize" pattern used by <see cref="JobStore"/>,
/// <see cref="UnitStore"/>, and <see cref="SettingsManager"/>.
/// </summary>
internal static class JsonStore
{
    internal static IDocumentStorage? Store =>
        MigrationJobContext.Instance?.Store;

    /// <summary>
    /// Reads <paramref name="path"/> from the document store and
    /// deserializes the JSON to <typeparamref name="T"/>. Returns
    /// <c>null</c> when the store is unavailable, the document is
    /// missing/empty, or deserialization yields <c>null</c>.
    /// </summary>
    internal static T? Read<T>(string path) where T : class
    {
        var store = Store;
        if (store == null) return null;
        var json = store.Read(path);
        if (string.IsNullOrEmpty(json)) return null;
        return JsonConvert.DeserializeObject<T>(json);
    }

    /// <summary>
    /// Serializes <paramref name="value"/> to JSON and writes it to
    /// <paramref name="path"/>. Returns the underlying
    /// <see cref="IDocumentStorage.Write"/> result, or <c>false</c>
    /// when the store is unavailable.
    /// </summary>
    internal static bool Write<T>(
        string path, T value, bool indented = true)
    {
        var store = Store;
        if (store == null) return false;
        var json = JsonConvert.SerializeObject(
            value, indented ? Formatting.Indented : Formatting.None);
        return store.Write(path, json);
    }
}
