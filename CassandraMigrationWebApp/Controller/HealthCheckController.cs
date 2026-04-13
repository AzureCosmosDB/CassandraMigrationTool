using Microsoft.AspNetCore.Mvc;

namespace CassandraMigrationWebApp.Controller;
[ApiController]
[Route("api/[controller]")]
public class HealthCheckController : ControllerBase
{
    [HttpGet("ping")]
    public IActionResult Ping()
    {
        return Ok(new { status = "healthy", timestamp = DateTime.UtcNow });
    }
}
