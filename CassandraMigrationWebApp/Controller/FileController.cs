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
}
