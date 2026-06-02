using Microsoft.AspNetCore.Mvc;
using CassandraMigrationProcessor.Context;
using CassandraMigrationWebApp.Service;

namespace CassandraMigrationWebApp.Controller;
[ApiController]
[Route("api/[controller]")]
public class KeepAliveController : ControllerBase
{
    private readonly JobManager _jobManager;
    private readonly MigrationJobContext _context;

    public KeepAliveController(JobManager jobManager, MigrationJobContext context)
    {
        _jobManager = jobManager;
        _context = context;
    }

    /// <summary>
    /// Keep alive endpoint that returns the time since the active migration job has been running
    /// </summary>
    /// <returns>Keep alive response with job runtime information</returns>
    [HttpGet]
    public IActionResult Get()
    {
        var runningJobId = _jobManager.GetRunningJobId();

        if (string.IsNullOrEmpty(runningJobId))
            return InactiveResponse("NoActiveJob", runningJobId: null);

        var activeJob = _context.CurrentlyActiveJob;

        if (activeJob == null || activeJob.Id != runningJobId)
            return InactiveResponse("JobNotFound", runningJobId);

        // Calculate runtime since job started
        double runtimeSeconds = 0;
        string runtimeFormatted = "N/A";

        if (activeJob.StartedOn.HasValue)
        {
            var runtime = DateTime.UtcNow - activeJob.StartedOn.Value;
            runtimeSeconds = runtime.TotalSeconds;
            runtimeFormatted = FormatTimeSpan(runtime);
        }

        var response = new
        {
            Status = "Active",
            Timestamp = DateTime.UtcNow,
            RunningJobId = runningJobId,
            JobName = activeJob.Name ?? "Unnamed",
            StartedOn = activeJob.StartedOn,
            RuntimeSeconds = runtimeSeconds,
            RuntimeFormatted = runtimeFormatted
        };

        return Ok(response);
    }

    /// <summary>
    /// Shared payload shape for the two "no live runtime" branches
    /// (no job running, or the running id no longer resolves to a
    /// loaded job). Keeps both responses byte-identical in field
    /// order and type so monitoring consumers see one stable schema.
    /// </summary>
    private IActionResult InactiveResponse(string status, string? runningJobId) => Ok(new
    {
        Status = status,
        Timestamp = DateTime.UtcNow,
        RunningJobId = runningJobId,
        RuntimeSeconds = 0,
        RuntimeFormatted = "N/A"
    });

    private static string FormatTimeSpan(TimeSpan timeSpan)
    {
        if (timeSpan.TotalDays >= 1)
            return $"{(int)timeSpan.TotalDays}d {timeSpan.Hours}h {timeSpan.Minutes}m";
        if (timeSpan.TotalHours >= 1)
            return $"{(int)timeSpan.TotalHours}h {timeSpan.Minutes}m {timeSpan.Seconds}s";
        if (timeSpan.TotalMinutes >= 1)
            return $"{(int)timeSpan.TotalMinutes}m {timeSpan.Seconds}s";
        return $"{(int)timeSpan.TotalSeconds}s";
    }
}
