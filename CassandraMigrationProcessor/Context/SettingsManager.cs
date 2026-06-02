using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Context;

/// <summary>
/// Loads and saves the global <see cref="AppSettings"/> document (<c>config.json</c>)
/// through <see cref="MigrationJobContext"/>'s document store.
/// </summary>
public static class SettingsManager
{
    public static void Load(AppSettings settings)
    {
        if (MigrationJobContext.Instance.Store == null) return;

        var loaded = JsonStore.Read<AppSettings>(JobStore.ConfigPath);
        if (loaded != null)
        {
            settings.ApplyLoaded(loaded);
            return;
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
            JsonStore.Write(JobStore.ConfigPath, settings, indented: false);
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
