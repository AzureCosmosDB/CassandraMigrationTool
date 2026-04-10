using Microsoft.AspNetCore.Mvc;
using CassandraMigrationProcessor;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Context;
using CassandraMigrationWebApp.Service;
using System.Text;

[ApiController]
[Route("api/[controller]")]
public class FileController : ControllerBase
{
    private readonly MigrationContextService _ctx;

    public FileController(MigrationContextService ctx)
    {
        _ctx = ctx;
    }

    [HttpGet("download/MigrationLog/{Id}")]
    public IActionResult DownloadFile(string Id)
    {
        var log = new MigrationLog();
        log.SetStorage(MigrationJobContext.CreateLogStorageCallbacks(_ctx.Store));
        var fileBytes = log.ExportLogsAsBytes(Id, 0, 0);
        return File(fileBytes, "application/octet-stream", $"{Id}.txt");
    }

    [HttpGet("download/migrationunit/{jobId}/{migrationUnitId}")]
    public IActionResult DownloadMigrationUnit(string jobId, string migrationUnitId)
    {
        var filePath = Path.Combine(JobStore.JobsFolder, jobId, $"{migrationUnitId}.json");

        if (_ctx.Store == null || !_ctx.Store.Exists(filePath))
        {
            return NotFound("Migration unit file not found.");
        }

        var jsonContent = _ctx.Store.Read(filePath);

        if (string.IsNullOrEmpty(jsonContent))
        {
            return NotFound("Migration unit file is empty or could not be read.");
        }

        var jsonObject = Newtonsoft.Json.JsonConvert.DeserializeObject(jsonContent);
        var prettyJson = Newtonsoft.Json.JsonConvert.SerializeObject(jsonObject, Newtonsoft.Json.Formatting.Indented);

        var fileBytes = Encoding.UTF8.GetBytes(prettyJson);
        var contentType = "application/json";

        return File(fileBytes, contentType, $"{migrationUnitId}.json");
    }

    [HttpGet("download/job/{jobId}")]
    public IActionResult DownloadJob(string jobId)
    {
        var job = _ctx.GetJob(jobId);

        if (job == null)
        {
            return NotFound("Job file not found.");
        }

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
            var countLog = new MigrationLog();
            countLog.SetStorage(MigrationJobContext.CreateLogStorageCallbacks(_ctx.Store));
            int count = countLog.GetLogCount(Id);
            return Ok(new { count });
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

            var pageLog = new MigrationLog();
            pageLog.SetStorage(MigrationJobContext.CreateLogStorageCallbacks(_ctx.Store));
            var fileBytes = pageLog.DownloadLogsPaginated(Id, skip, pageSize);
            var contentType = "application/octet-stream";
            return File(fileBytes, contentType, $"{Id}_page_{pageNumber}.txt");
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }
}
