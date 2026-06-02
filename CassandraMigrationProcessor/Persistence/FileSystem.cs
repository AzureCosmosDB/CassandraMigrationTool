namespace CassandraMigrationProcessor.Persistence;
/// <summary>
/// Abstraction for file system operations. Local disk only.
/// </summary>
public static class FileSystem
{
    public static void EnsureDirectoryExists(string path)
    {
        if (!string.IsNullOrEmpty(path)
            && !Directory.Exists(path))
            Directory.CreateDirectory(path);
    }

    public static bool WriteAllText(
        string path, string content)
    {
        File.WriteAllText(path, content);
        return true;
    }

    public static string ReadAllText(string path)
    {
        return File.ReadAllText(path);
    }

    public static bool Exists(string path)
    {
        return File.Exists(path) || Directory.Exists(path);
    }

    public static void DeleteIfExists(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }

    public static bool DeleteDirectory(
        string path, bool recursive = false)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, recursive);
            return true;
        }
        return false;
    }

    public static List<string> ListFiles(
        string directory,
        string searchPattern,
        bool recursive = false)
    {
        if (!Directory.Exists(directory))
            return new List<string>();

        var option = recursive
            ? SearchOption.AllDirectories
            : SearchOption.TopDirectoryOnly;
        return Directory.GetFiles(
            directory, searchPattern, option).ToList();
    }

    public static FileStream OpenAppend(string path)
    {
        return new FileStream(
            path, FileMode.Append,
            FileAccess.Write, FileShare.Read);
    }

    public static FileStream OpenReadShared(string path)
    {
        return new FileStream(
            path, FileMode.Open,
            FileAccess.Read, FileShare.ReadWrite);
    }

    public static void CopyFile(
        string source, string destination)
    {
        File.Copy(source, destination, overwrite: true);
    }
}
