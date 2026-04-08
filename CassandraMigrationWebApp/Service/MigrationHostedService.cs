using Microsoft.Extensions.Hosting;
using CassandraMigrationProcessor;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers;

namespace CassandraMigrationWebApp.Service
{
    /// <summary>
    /// Background service placeholder. Auto-resume has been
    /// removed — jobs must be resumed manually by the user.
    /// </summary>
    public class MigrationHostedService : BackgroundService
    {
        private readonly JobManager _jobManager;
        private readonly ILogger<MigrationHostedService> _logger;

        public MigrationHostedService(
            JobManager jobManager,
            ILogger<MigrationHostedService> logger)
        {
            _jobManager = jobManager;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(
            CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "MigrationHostedService started at {Time}",
                DateTime.UtcNow);

            // No auto-resume — jobs are only started by
            // explicit user action.
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
