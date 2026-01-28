// File: Data/ApplicationDbContextFactory.cs
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;
using System.IO;

namespace FinanceApi.Data
{
    public class ApplicationDbContextFactory : IDesignTimeDbContextFactory<ApplicationDbContext>
    {
        public ApplicationDbContext CreateDbContext(string[] args)
        {
            // dotnet ef runs from the current working directory; read appsettings from there
            var cfg = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true)
                .AddJsonFile("appsettings.Development.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            // Change "Default" to your real key if needed (e.g., "DefaultConnection")
            var cs = cfg.GetConnectionString("Default")
                     ?? "Server=.;Database=FinanceDb;Trusted_Connection=True;TrustServerCertificate=True";

            return new ApplicationDbContext(
                new DbContextOptionsBuilder<ApplicationDbContext>()
                    .UseSqlServer(cs)   // If you use PostgreSQL, change to .UseNpgsql(cs)
                    .Options
            );
        }
    }
}
