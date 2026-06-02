namespace CassandraMigrationProcessor.Persistence;
/// <summary>
/// Contract for document CRUD operations.
/// </summary>
public interface IDocumentStorage
{
    void Initialize(string connectionStringOrPath);
    bool Write(string id, string jsonContent);
    string? Read(string id);
    bool Exists(string id);
    bool Delete(string id);
    List<string> ListIds();
}
