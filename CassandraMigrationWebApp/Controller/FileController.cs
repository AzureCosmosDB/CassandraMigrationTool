using Microsoft.AspNetCore.Mvc;
using CassandraMigrationProcessor.Context;
using System.Text;

[ApiController]
[Route("api/[controller]")]
public class FileController : ControllerBase
{
    private readonly MigrationJobContext _context;

    public FileController(MigrationJobContext context)
    {
        _context = context;
    }

    [HttpGet("download/TableMigration/{jobId}/{migrationUnitId}")]
    public IActionResult DownloadMigrationUnit(string jobId, string migrationUnitId)
    {
        var filePath = Path.Combine(JobStore.JobsFolder, jobId, $"{migrationUnitId}.json");

        if (_context.Store == null || !_context.Store.Exists(filePath))
        {
            return NotFound("Migration unit file not found.");
        }

        var jsonContent = _context.Store.Read(filePath);

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
        var job = _context.GetMigrationJob(jobId);

        if (job == null)
        {
            return NotFound("Job file not found.");
        }

        var prettyJson = Newtonsoft.Json.JsonConvert.SerializeObject(job, Newtonsoft.Json.Formatting.Indented);

        var fileBytes = Encoding.UTF8.GetBytes(prettyJson);
        var contentType = "application/json";

        return File(fileBytes, contentType, $"{jobId}.json");
    }

    // Streams the persisted log for a job (or backup file name) back to
    // the MigrationJobViewer's Downloader() click. ExportLogsAsBytes
    // returns an empty array when no entries exist.
    [HttpGet("download/log/{fileName}")]
    public IActionResult DownloadLog(string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            return BadRequest("fileName is required.");

        // Defence in depth: refuse any path-traversal attempt. Job IDs and
        // backup filenames are simple alphanum/hyphen/underscore/dot tokens.
        if (fileName.IndexOfAny(new[] { '/', '\\', ':' }) >= 0
            || fileName.Contains(".."))
        {
            return BadRequest("Invalid fileName.");
        }

        if (_context.LogStore == null)
            return NotFound("Log storage is not initialised.");

        byte[] bytes;
        try
        {
            // Use large but bounded limits to avoid unbounded memory allocation
            const int maxEntries = 500_000;
            bytes = _context.LogStore.ExportLogsAsBytes(fileName, maxEntries, maxEntries);
        }
        catch (Exception ex)
        {
            return Problem(
                detail: $"Failed to export log '{fileName}': {ex.GetType().Name}: {ex.Message}",
                statusCode: 500);
        }

        if (bytes == null || bytes.Length == 0)
            return NotFound($"No log entries found for '{fileName}'.");

        return File(bytes, "text/plain", $"{fileName}.log");
    }

    [HttpGet("download/log/{jobId}/page/{pageNumber}/{pageSize}")]
    public IActionResult DownloadLogPage(string jobId, int pageNumber, int pageSize)
    {
        if (string.IsNullOrWhiteSpace(jobId))
            return BadRequest("jobId is required.");

        if (jobId.IndexOfAny(new[] { '/', '\\', ':' }) >= 0 || jobId.Contains(".."))
            return BadRequest("Invalid jobId.");

        if (pageNumber < 1 || pageSize < 1)
            return BadRequest("pageNumber and pageSize must be positive.");

        if (pageSize > 10_000)
            return BadRequest("pageSize cannot exceed 10000.");

        if (_context.LogStore == null)
            return NotFound("Log storage is not initialised.");

        long skip = (long)(pageNumber - 1) * pageSize;
        if (skip > int.MaxValue)
            return BadRequest("Page offset too large.");

        byte[] bytes;
        try
        {
            bytes = _context.LogStore.DownloadLogsPaginated(jobId, (int)skip, pageSize);
        }
        catch (Exception ex)
        {
            return Problem(
                detail: $"Failed to export log page for '{jobId}': {ex.GetType().Name}: {ex.Message}",
                statusCode: 500);
        }

        if (bytes == null || bytes.Length == 0)
            return NotFound($"No log entries found for page {pageNumber}.");

        return File(bytes, "text/plain", $"{jobId}_page{pageNumber}.log");
    }
}
