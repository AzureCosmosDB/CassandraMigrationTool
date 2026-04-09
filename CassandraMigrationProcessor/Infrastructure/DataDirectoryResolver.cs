using CassandraMigrationProcessor.Context;
using System;
using System.IO;

namespace CassandraMigrationProcessor.Infrastructure
{
    public static class DataDirectoryResolver
    {
        static string _workingFolder = string.Empty;

        public static string GetWorkingFolder()
        {
            if (!string.IsNullOrEmpty(_workingFolder))
                return _workingFolder;

            if (!IsWindows())
            {
                _workingFolder =
                    $"{Environment.GetEnvironmentVariable("ResourceDrive")}/" +
                    $"{MigrationJobContext.AppId}/";
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
}
