# Cassandra Migration Tool — Web App container image.
#
# This is the in-project Visual Studio Dockerfile used by `Add Docker
# Support`. The ACA-targeted variant lives at the repo root
# (Dockerfile.aca) and uses the same project layout; keep the two
# files in sync when changing the build/publish steps.
#
# Build context (from repo root):
#     docker build -f CassandraMigrationWebApp/Dockerfile -t cmt-web .

# Runtime base. The web app is an ASP.NET Core Blazor Server app
# listening on 8080 (HTTP) and 8081 (HTTPS).
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

# Restore + build using the .NET SDK image. The two project files
# are copied first so `dotnet restore` can populate the NuGet cache
# layer; the full source is copied in only after restore so an
# unrelated source change does not invalidate the (large) restore
# layer.
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["CassandraMigrationWebApp/CassandraMigrationWebApp.csproj", "CassandraMigrationWebApp/"]
COPY ["CassandraMigrationProcessor/CassandraMigrationProcessor.csproj", "CassandraMigrationProcessor/"]
RUN dotnet restore "./CassandraMigrationWebApp/CassandraMigrationWebApp.csproj"
COPY . .
WORKDIR "/src/CassandraMigrationWebApp"
RUN dotnet build "./CassandraMigrationWebApp.csproj" -c $BUILD_CONFIGURATION -o /app/build

# Publish stage: produces the framework-dependent deployable.
# /p:UseAppHost=false skips the native launcher so we run via
# `dotnet CassandraMigrationWebApp.dll` from the ENTRYPOINT below
# (smaller image and no apphost executable to chmod / sign).
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./CassandraMigrationWebApp.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# Final runtime image. The app's StateStoreDiskPersistence writes
# job metadata, per-job logs, and the migration UnitStore under
# /app/migration-data — pre-create the directory with the right
# ownership so the non-root $APP_UID user can write to it on first
# launch (Azure Container Apps storage mounts also bind their
# Azure Files share at /app/migration-data; see Dockerfile.aca).
FROM base AS final
USER root
RUN mkdir -p /app/migration-data/home \
    && chown -R $APP_UID:$APP_UID /app/migration-data \
    && chmod -R 755 /app/migration-data
USER $APP_UID

WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "CassandraMigrationWebApp.dll"]
