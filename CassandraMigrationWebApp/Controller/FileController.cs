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

        // The backup-file download path passes a filename that already
        // carries the ".bin" extension, but the log store appends ".bin"
        // internally. Strip a trailing ".bin" so both the job-id and the
        // backup-file forms resolve to the correct file (otherwise the
        // store looks for "...bin.bin" and the download 404s).
        var logId = fileName.EndsWith(".bin", StringComparison.OrdinalIgnoreCase)
            ? fileName[..^4]
            : fileName;

        // Bound the full-log download. Below the cap we return the complete
        // log (top = cap, bottom = 0 → no top/bottom sampling, so the middle
        // is never silently dropped); above it, direct callers to the
        // paginated endpoint rather than allocating an unbounded buffer.
        const int maxEntries = 1_000_000;
        int entryCount;
        try
        {
            entryCount = _context.LogStore.GetLogCount(logId);
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or ArgumentException or ObjectDisposedException)
        {
            return Problem(
                detail: $"Failed to read log '{fileName}': {ex.GetType().Name}: {ex.Message}",
                statusCode: 500);
        }

        if (entryCount > maxEntries)
            return Problem(
                detail: $"Log '{fileName}' has {entryCount:N0} entries, exceeding the {maxEntries:N0}-entry download cap. " +
                        "Use the paginated endpoint /api/File/download/log/{jobId}/page/{pageNumber}/{pageSize} instead.",
                statusCode: 413);

        byte[] bytes;
        try
        {
            bytes = _context.LogStore.ExportLogsAsBytes(logId, maxEntries, 0);
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or ArgumentException or ObjectDisposedException)
        {
            return Problem(
                detail: $"Failed to export log '{fileName}': {ex.GetType().Name}: {ex.Message}",
                statusCode: 500);
        }

        if (bytes == null || bytes.Length == 0)
            return NotFound($"No log entries found for '{fileName}'.");

        return File(bytes, "text/plain; charset=utf-8", $"{logId}.log");
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
        catch (Exception ex) when (ex is IOException or InvalidOperationException or ArgumentException or ObjectDisposedException)
        {
            return Problem(
                detail: $"Failed to export log page for '{jobId}': {ex.GetType().Name}: {ex.Message}",
                statusCode: 500);
        }

        if (bytes == null || bytes.Length == 0)
            return NotFound($"No log entries found for page {pageNumber}.");

        return File(bytes, "text/plain; charset=utf-8", $"{jobId}_page{pageNumber}.log");
    }
}
