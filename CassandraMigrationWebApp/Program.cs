using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using CassandraMigrationWebApp.Service;
using CassandraMigrationProcessor;
using System.Security.AccessControl;
using Microsoft.AspNetCore.Components.Authorization;
using CassandraMigrationProcessor.Context;

var builder = WebApplication.CreateBuilder(args);

// Set up file-based diagnostic logging (stdout capture unreliable on IIS in-process)
var diagLogPath = Path.Combine(
    Environment.GetEnvironmentVariable("HOME") ?? ".",
    "LogFiles", "app-diag.log");
try
{
    Directory.CreateDirectory(Path.GetDirectoryName(diagLogPath)!);
    var diagStream = new StreamWriter(diagLogPath, append: true) { AutoFlush = true };
    Console.SetOut(diagStream);
    Console.SetError(diagStream);
    Console.WriteLine($"=== App starting at {DateTime.UtcNow:O} ===");
}
catch { /* fall back to default stdout */ }

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

//Map environment variables to configuration keys
var stateStoreCSorPath = Environment.GetEnvironmentVariable("StateStoreConnectionStringOrPath");
if (!string.IsNullOrEmpty(stateStoreCSorPath))
{
    builder.Configuration["StateStore:ConnectionStringOrPath"] = stateStoreCSorPath;
}

var appId = Environment.GetEnvironmentVariable("StateStoreAppID");
if (!string.IsNullOrEmpty(appId))
{
    builder.Configuration["StateStore:AppID"] = appId;
    MigrationJobContext.AppId = appId;
}


var useLocalDisk=Environment.GetEnvironmentVariable("StateStoreUseLocalDisk");
bool useLocal=false;
if (!string.IsNullOrEmpty(useLocalDisk))
{
    bool.TryParse(useLocalDisk, out useLocal);
    builder.Configuration["StateStore:UseLocalDisk"] = useLocal.ToString();
}

builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddSingleton(builder.Configuration);
builder.Services.AddSingleton<JobManager>();
builder.Services.AddScoped<FileService>();

// Background service: keeps migration alive across
// IIS app pool recycles and auto-resumes jobs.
builder.Services.AddHostedService<MigrationHostedService>();

// Add authentication services
builder.Services.AddSingleton<PasswordManager>();
builder.Services.AddScoped<AuthenticationService>();
builder.Services.AddScoped<AuthenticationStateProvider, CustomAuthenticationStateProvider>();
builder.Services.AddAuthorizationCore();

var app = builder.Build();

// _configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
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

app.MapControllers(); // Ensure controllers are mapped

app.Run();

