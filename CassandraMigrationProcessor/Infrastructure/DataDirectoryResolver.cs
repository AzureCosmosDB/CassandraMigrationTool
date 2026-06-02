namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Resolves the working folder where the processor reads and writes
/// migration state (job registry, jobdefinition.json, logs), honoring
/// platform conventions (Windows <c>ResourceDrive</c> vs Linux
/// app-scoped path) and the configured app ID.
/// </summary>
public static class DataDirectoryResolver
{
    static string _workingFolder = string.Empty;
    private static string? _appId;

    /// <summary>
    /// Set the application ID for Linux working folder path.
    /// Must be called before GetWorkingFolder on non-Windows.
    /// </summary>
    public static void SetAppId(string? appId)
    {
        _appId = appId;
    }

    public static string GetWorkingFolder()
    {
        if (!string.IsNullOrEmpty(_workingFolder))
            return _workingFolder;

        if (!IsWindows())
        {
            var resourceDrive = Environment.GetEnvironmentVariable("ResourceDrive");
            var stateStore = Environment.GetEnvironmentVariable("StateStoreConnectionStringOrPath");
            if (!string.IsNullOrEmpty(resourceDrive) && !string.IsNullOrEmpty(_appId))
            {
                _workingFolder = $"{resourceDrive}/{_appId}/";
            }
            else if (!string.IsNullOrEmpty(stateStore))
            {
                _workingFolder = stateStore.EndsWith("/") ? stateStore : stateStore + "/";
            }
            else
            {
                _workingFolder = "/tmp/migration-data/";
            }
            if (!Directory.Exists(_workingFolder))
                Directory.CreateDirectory(_workingFolder);
            MigrationUtilities.LogToFile($"WorkingFolder (Linux): {_workingFolder}");
            return _workingFolder;
        }

        if (Directory.Exists(
            $"{Path.GetTempPath()}migrationjobs"))
        {
            _workingFolder = Path.GetTempPath();
            MigrationUtilities.LogToFile($"WorkingFolder (Temp): {_workingFolder}");
            return _workingFolder;
        }

        // Azure App Service (Windows) -- only D:\home\ is writable.
        // Operators on App Service who don't override StateStoreConnectionStringOrPath
        // would otherwise land in C:\Users\... (no access) and the
        // host process exits with 0xe0434352 before serving a request.
        // Detect via WEBSITE_INSTANCE_ID (set on every App Service worker)
        // and pick the writable scratch directory automatically.
        var websiteInstanceId = Environment.GetEnvironmentVariable("WEBSITE_INSTANCE_ID");
        if (!string.IsNullOrEmpty(websiteInstanceId))
        {
            _workingFolder = "D:\\home\\MigrationDrive\\";
            MigrationUtilities.LogToFile(
                $"WorkingFolder (Azure App Service Windows): {_workingFolder} (WEBSITE_INSTANCE_ID={websiteInstanceId})");
            return _workingFolder;
        }

        string homePath =
            Environment.GetEnvironmentVariable("ResourceDrive");

        if (string.IsNullOrEmpty(homePath))
            _workingFolder = Path.GetTempPath();

        if (!string.IsNullOrEmpty(homePath)
            && Directory.Exists(
                Path.Combine(homePath, "home\\")))
        {
            _workingFolder = Path.Combine(homePath, "home\\");
        }

        MigrationUtilities.LogToFile($"WorkingFolder (Win): {_workingFolder} (ResourceDrive={homePath})");
        return _workingFolder;
    }

    public static bool IsWindows()
    {
        return Environment.OSVersion.Platform
            == PlatformID.Win32NT;
    }
}
