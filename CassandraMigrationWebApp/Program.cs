using Microsoft.AspNetCore.Components;
using CassandraMigrationWebApp.Service;
using Microsoft.AspNetCore.Components.Authorization;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Context;

var builder = WebApplication.CreateBuilder(args);

// Set up file-based diagnostic logging (stdout capture unreliable on IIS in-process)
var diagLogPath = Path.Combine(
    Environment.GetEnvironmentVariable("HOME") ?? ".",
    "LogFiles", "app-diag.log");
StreamWriter? diagStream = null;
try
{
    Directory.CreateDirectory(Path.GetDirectoryName(diagLogPath)!);
    diagStream = new StreamWriter(diagLogPath, append: true) { AutoFlush = true };
    Console.SetOut(diagStream);
    Console.SetError(diagStream);
    Console.WriteLine($"=== App starting at {DateTime.UtcNow:O} ===");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[WARN] Diagnostic MigrationLog setup failed, falling back to stdout: {ex.Message}");
}

// Surface unhandled exceptions to the redirected diag stream so they
// reach App Service logs before the host exits (default 0xe0434352
// fatal exit produces no captured stack trace otherwise).
AppDomain.CurrentDomain.UnhandledException += (sender, args) =>
{
    try
    {
        // Log the ExceptionObject directly: it is usually an Exception
        // (whose ToString gives the full stack) but can be a non-Exception
        // payload, so casting to Exception could lose details or log null.
        Console.Error.WriteLine(
            $"[FATAL] [{DateTime.UtcNow:O}] AppDomain.UnhandledException (terminating={args.IsTerminating}): {args.ExceptionObject}");
    }
    catch (Exception ex) when (ex is IOException or ObjectDisposedException) { /* never let the handler itself throw */ }
};
TaskScheduler.UnobservedTaskException += (sender, args) =>
{
    try
    {
        Console.Error.WriteLine(
            $"[ERROR] [{DateTime.UtcNow:O}] TaskScheduler.UnobservedTaskException: {args.Exception}");
        args.SetObserved();
    }
    catch (Exception ex) when (ex is IOException or ObjectDisposedException) { /* never let the handler itself throw */ }
};

builder.Services.AddControllersWithViews();

// Register HttpClient and dynamically set the base address using NavigationManager
builder.Services.AddScoped(sp =>
{
    // Retrieve NavigationManager from the service provider
    var navigationManager = sp.GetRequiredService<NavigationManager>();

    // Create and configure HttpClient with dynamic base address
    var client = new HttpClient
    {
        BaseAddress = new Uri(navigationManager.BaseUri)  // Use NavigationManager's BaseUri
    };

    return client;
});

builder.Configuration.AddEnvironmentVariables();

// Map environment variables to configuration keys
var stateStoreCSorPath = Environment.GetEnvironmentVariable("StateStoreConnectionStringOrPath");
if (!string.IsNullOrEmpty(stateStoreCSorPath))
{
    builder.Configuration["StateStore:ConnectionStringOrPath"] = stateStoreCSorPath;
}

var appId = Environment.GetEnvironmentVariable("StateStoreAppID");
if (!string.IsNullOrEmpty(appId))
{
    builder.Configuration["StateStore:AppID"] = appId;
    DataDirectoryResolver.SetAppId(appId);
}

var useLocalDisk = Environment.GetEnvironmentVariable("StateStoreUseLocalDisk");
bool useLocal = false;
if (!string.IsNullOrEmpty(useLocalDisk))
{
    bool.TryParse(useLocalDisk, out useLocal);
    builder.Configuration["StateStore:UseLocalDisk"] = useLocal.ToString();
}

builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();

// Create and initialize the MigrationJobContext singleton
var migrationJobContext = new MigrationJobContext();
migrationJobContext.Initialize(builder.Configuration);
builder.Services.AddSingleton(migrationJobContext);

builder.Services.AddSingleton<JobManager>();

// Add authentication services
builder.Services.AddSingleton<PasswordManager>();
builder.Services.AddScoped<AuthenticationService>();
builder.Services.AddScoped<AuthenticationStateProvider, CustomAuthenticationStateProvider>();
builder.Services.AddAuthorizationCore();

var app = builder.Build();

// Register disposal of diagnostic MigrationLog stream on shutdown
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
if (diagStream != null)
{
    lifetime.ApplicationStopping.Register(() =>
    {
        try { diagStream.Dispose(); }
        catch { /* best-effort cleanup */ }
    });
}

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseStaticFiles();

app.UseRouting();

app.UseAuthentication();
app.UseAuthorization();

app.MapBlazorHub();
app.MapGet("/healthz", () => Results.Ok("ok"));
app.MapFallbackToPage("/_Host");

app.MapControllers();

app.Run();
