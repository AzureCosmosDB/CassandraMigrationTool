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
                // Azure App Service Linux: /tmp is wiped on container
                // recycle. $HOME is the persistent /home mount.
                var linuxWebsiteInstanceId = Environment.GetEnvironmentVariable("WEBSITE_INSTANCE_ID");
                var home = Environment.GetEnvironmentVariable("HOME");
                if (!string.IsNullOrEmpty(linuxWebsiteInstanceId) && !string.IsNullOrEmpty(home))
                {
                    _workingFolder = home.TrimEnd('/') + "/migration-data/";
                    MigrationUtilities.LogToFile(
                        $"WorkingFolder (Azure App Service Linux): {_workingFolder} (WEBSITE_INSTANCE_ID={linuxWebsiteInstanceId})");
                }
                else
                {
                    _workingFolder = "/tmp/migration-data/";
                }
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

        if (!string.IsNullOrEmpty(homePath)
            && Directory.Exists(
                Path.Combine(homePath, "home\\")))
        {
            _workingFolder = Path.Combine(homePath, "home\\");
        }
        else
        {
            // Previous logic returned an empty string when
            // ResourceDrive was set but %ResourceDrive%\home\ didn't
            // exist (neither the empty-check nor the directory-exists
            // branch fired). Downstream Path.Combine("", ...) then wrote
            // job state into the process working directory — potentially
            // a non-writable system folder, causing silent state loss
            // across restarts. Use a dedicated subfolder under TEMP so
            // multiple installs don't collide.
            _workingFolder = Path.Combine(Path.GetTempPath(), "CassandraMigrationTool\\");
            if (!Directory.Exists(_workingFolder))
                Directory.CreateDirectory(_workingFolder);
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
