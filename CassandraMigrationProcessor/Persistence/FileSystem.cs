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

    private const int MaxSharingRetries = 6;

    /// <summary>
    /// Writes <paramref name="content"/> to a temporary sibling file and
    /// moves it over <paramref name="path"/>, so readers never observe a
    /// partially written document. Retries briefly while another process
    /// (e.g. the previous worker during an App Service restart) holds the
    /// file.
    /// </summary>
    public static bool WriteAllText(
        string path, string content)
    {
        var tempPath = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            File.WriteAllText(tempPath, content);
            RetryOnSharingViolation(() => File.Move(tempPath, path, overwrite: true));
            return true;
        }
        finally
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
    }

    /// <summary>
    /// Reads the whole file while allowing concurrent writers/replacers,
    /// retrying briefly on transient sharing violations.
    /// </summary>
    public static string ReadAllText(string path)
    {
        return RetryOnSharingViolation(() =>
        {
            using var stream = new FileStream(
                path, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        });
    }

    private static void RetryOnSharingViolation(Action action)
        => RetryOnSharingViolation(() => { action(); return true; });

    private static T RetryOnSharingViolation<T>(Func<T> action)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return action();
            }
            catch (IOException ex) when (
                attempt < MaxSharingRetries
                && ex is not FileNotFoundException
                && ex is not DirectoryNotFoundException)
            {
                Thread.Sleep(50 * attempt);
            }
            catch (UnauthorizedAccessException) when (attempt < MaxSharingRetries)
            {
                // Windows reports a pending replace/delete as access denied.
                Thread.Sleep(50 * attempt);
            }
        }
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
