using Microsoft.AspNetCore.Mvc;
using CassandraMigrationWebApp.Service;
using CassandraMigrationProcessor.Context;

namespace CassandraMigrationWebApp.Controller
{
    [ApiController]
    [Route("api/[controller]")]
    public class KeepAliveController : ControllerBase
    {
        // JobManager is registered as a singleton in Program.cs, so this will be the shared instance
        private readonly JobManager _jobManager;

        public KeepAliveController(JobManager jobManager)
        {
            _jobManager = jobManager;
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
            {
                return Ok(new
                {
                    Status = "NoActiveJob",
                    Timestamp = DateTime.UtcNow,
                    RunningJobId = (string?)null,
                    RuntimeSeconds = 0,
                    RuntimeFormatted = "N/A"
                });
            }

            // Get the active job from MigrationJobContext
            var activeJob = MigrationJobContext.CurrentlyActiveJob;

            if (activeJob == null || activeJob.Id != runningJobId)
            {
                return Ok(new
                {
                    Status = "JobNotFound",
                    Timestamp = DateTime.UtcNow,
                    RunningJobId = runningJobId,
                    RuntimeSeconds = 0,
                    RuntimeFormatted = "N/A"
                });
            }

            // Calculate runtime since job started
            TimeSpan runtime = TimeSpan.Zero;
            double runtimeSeconds = 0;
            string runtimeFormatted = "N/A";

            if (activeJob.StartedOn.HasValue)
            {
                runtime = DateTime.UtcNow - activeJob.StartedOn.Value;
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

        private string FormatTimeSpan(TimeSpan timeSpan)
        {
            if (timeSpan.TotalDays >= 1)
            {
                return $"{(int)timeSpan.TotalDays}d {timeSpan.Hours}h {timeSpan.Minutes}m";
            }
            else if (timeSpan.TotalHours >= 1)
            {
                return $"{(int)timeSpan.TotalHours}h {timeSpan.Minutes}m {timeSpan.Seconds}s";
            }
            else if (timeSpan.TotalMinutes >= 1)
            {
                return $"{(int)timeSpan.TotalMinutes}m {timeSpan.Seconds}s";
            }
            else
            {
                return $"{(int)timeSpan.TotalSeconds}s";
            }
        }
    }
}
