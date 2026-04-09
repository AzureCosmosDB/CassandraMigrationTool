using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Mvc;
using CassandraMigrationProcessor;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Context;
using System.Text;

[ApiController]
[Route("api/[controller]")]

public class FileController : ControllerBase
{


    [HttpGet("download/MigrationLog/{Id}")]
    public IActionResult DownloadFile(string Id)
    {
        string fileSharePath = $"{WorkingFolderResolver.GetWorkingFolder()}migrationlogs"; // UNC path to your file share
        string filePath;

        filePath = Path.Combine(fileSharePath, Id + ".bin");

        var fileBytes = new MigrationLog().DownloadLogsAsJsonBytes(Id, 0, 0);
        var contentType = "application/octet-stream";
        return File(fileBytes, contentType, $"{Id}.txt");

    }

    [HttpGet("download/migrationunit/{jobId}/{migrationUnitId}")]
    public IActionResult DownloadMigrationUnit(string jobId, string migrationUnitId)
    {
        var filePath = Path.Combine(JobStore.JobsFolder, jobId, $"{migrationUnitId}.json");

        // Use the persistence storage to read the document
        if (MigrationJobContext.Store == null || !MigrationJobContext.Store.Exists(filePath))
        {
            return NotFound("Migration unit file not found.");
        }

        var jsonContent = MigrationJobContext.Store.Read(filePath);

        if (string.IsNullOrEmpty(jsonContent))
        {
            return NotFound("Migration unit file is empty or could not be read.");
        }

        // Pretty print the JSON
        var jsonObject = Newtonsoft.Json.JsonConvert.DeserializeObject(jsonContent);
        var prettyJson = Newtonsoft.Json.JsonConvert.SerializeObject(jsonObject, Newtonsoft.Json.Formatting.Indented);

        var fileBytes = Encoding.UTF8.GetBytes(prettyJson);
        var contentType = "application/json";

        return File(fileBytes, contentType, $"{migrationUnitId}.json");
    }

    [HttpGet("download/job/{jobId}")]
    public IActionResult DownloadJob(string jobId)
    {
        var job = MigrationJobContext.GetMigrationJob(jobId);

        if (job == null)
        {
            return NotFound("Job file not found.");
        }

        // Pretty print the JSON
        var prettyJson = Newtonsoft.Json.JsonConvert.SerializeObject(job, Newtonsoft.Json.Formatting.Indented);

        var fileBytes = Encoding.UTF8.GetBytes(prettyJson);
        var contentType = "application/json";

        return File(fileBytes, contentType, $"{jobId}.json");
    }

    [HttpGet("download/MigrationLog/{Id}/count")]
    public IActionResult GetLogCount(string Id)
    {
        try
        {
            MigrationLog MigrationLog = new MigrationLog();
            int count = MigrationLog.GetLogCount(Id);
            return Ok(new { count = count });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }

    [HttpGet("download/MigrationLog/{Id}/page/{pageNumber}/{pageSize}")]
    public IActionResult DownloadLogPage(string Id, int pageNumber, int pageSize)
    {
        try
        {
            // Calculate skip/take for pagination
            int skip = (pageNumber - 1) * pageSize;

            var fileBytes = new MigrationLog().DownloadLogsPaginated(Id, skip, pageSize);
            var contentType = "application/octet-stream";
            return File(fileBytes, contentType, $"{Id}_page_{pageNumber}.txt");
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }
}
