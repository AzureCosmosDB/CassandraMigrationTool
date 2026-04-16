using Newtonsoft.Json;
using CassandraMigrationProcessor.Models;
using System;

namespace CassandraMigrationProcessor.Context;
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
            string json = MigrationJobContext.Instance.Store.Read(filePath);
            var loaded =
                JsonConvert.DeserializeObject<AppSettings>(json);
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
            string json = JsonConvert.SerializeObject(settings);
            MigrationJobContext.Instance.Store.Write(
                GetFilePath(), json);
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
