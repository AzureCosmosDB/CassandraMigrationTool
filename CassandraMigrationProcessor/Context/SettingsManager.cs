using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Context;

/// <summary>
/// Loads and saves the global <see cref="AppSettings"/> document (<c>config.json</c>)
/// through <see cref="MigrationJobContext"/>'s document store.
/// </summary>
public static class SettingsManager
{
    private static string GetFilePath()
        => $"{JobStore.JobsFolder}\\config.json";

    public static void Load(AppSettings settings)
    {
        if (MigrationJobContext.Instance.Store == null) return;

        var filePath = GetFilePath();
        if (MigrationJobContext.Instance.Store.Exists(filePath))
        {
            var loaded = JsonStore.Read<AppSettings>(filePath);
            if (loaded != null)
            {
                settings.ApplyLoaded(loaded);
                return;
            }
        }

        settings.ApplyDefaults();
    }

    public static bool Save(
        AppSettings settings, out string errorMessage)
    {
        if (MigrationJobContext.Instance.Store == null)
        {
            errorMessage = "Store not initialized";
            return false;
        }

        try
        {
            JsonStore.Write(GetFilePath(), settings, indented: false);
            errorMessage = string.Empty;
            return true;
        }
        catch (Exception ex)
        {
            errorMessage = $"Error saving data: {ex}";
            return false;
        }
    }
}
