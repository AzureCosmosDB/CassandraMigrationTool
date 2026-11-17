using CassandraMigrationProcessor.Persistence;

namespace CassandraMigrationProcessor.Tests;

public sealed class FileSystemTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "cmt-fs-" + Guid.NewGuid().ToString("N"));

    public FileSystemTests() => Directory.CreateDirectory(_dir);

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    [Fact]
    public void ReadAllTextSucceedsWhileAnotherHandleHasTheFileOpenForWriting()
    {
        var path = Path.Combine(_dir, "JobRegistry.json");
        File.WriteAllText(path, "{\"jobs\":[]}");

        using var writer = new FileStream(
            path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);

        Assert.Equal("{\"jobs\":[]}", FileSystem.ReadAllText(path));
    }

    [Fact]
    public void WriteAllTextReplacesTheFileAndLeavesNoTemporaryFiles()
    {
        var path = Path.Combine(_dir, "doc.json");
        FileSystem.WriteAllText(path, "first");
        FileSystem.WriteAllText(path, "second");

        Assert.Equal("second", FileSystem.ReadAllText(path));
        Assert.Equal([path], Directory.GetFiles(_dir));
    }
}
