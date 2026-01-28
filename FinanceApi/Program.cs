using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using FinanceApi.Repositories;
using FinanceApi.Services;
using FinanceApi.Swagger;
using FluentScheduler;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using System.Data;
using System.Text;
using UnityWorksERP.Finance.AR;

var vuexyPath = Path.Combine(AppContext.BaseDirectory, "wwwroot", "dist", "vuexy");
var plainWwwroot = Path.Combine(AppContext.BaseDirectory, "wwwroot");
var resolvedWebRoot = Directory.Exists(vuexyPath) ? vuexyPath : plainWwwroot;

var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    WebRootPath = resolvedWebRoot
});

// ===================== Services =====================

builder.Services.AddDbContext<ApplicationDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.AddScoped<IDbConnectionFactory, SqlDbConnectionFactory>();
builder.Services.Configure<SmtpEmailSettings>(
    builder.Configuration.GetSection("EmailSettings"));
builder.Services.Scan(scan => scan
    .FromAssemblyOf<ICustomerService>()
        .AddClasses(c => c.Where(t => t.Name.EndsWith("Repository")))
            .AsImplementedInterfaces()
            .WithScopedLifetime()
        .AddClasses(c => c.Where(t => t.Name.EndsWith("Service")))
            .AsImplementedInterfaces()
            .WithScopedLifetime()
);

builder.Services.AddScoped<IPurchaseGoodReceiptRepository, PurchaseGoodReceiptRepository>();
builder.Services.AddScoped<ICatagoryRepository, CatagoryRepository>();
builder.Services.AddScoped<IcostingMethodRepository, CostingMethodRepository>();
builder.Services.AddScoped<IIncotermsRepository, IncotermsRepository>();
builder.Services.AddScoped<IFlagIssuesRepository, FlagIssuesRepository>();
builder.Services.AddScoped<ISupplierInvoicePinRepository, SupplierInvoicePinRepository>();
builder.Services.AddScoped<IPurchaseRequestTempRepository, PurchaseRequestTempRepository>();
builder.Services.AddScoped<IStockIssuesRepository, StockIssuesRepository>();
builder.Services.AddScoped<IBinRepository, BinRepository>();
builder.Services.AddScoped<IQuotationRepository, QuotationRepository>();
builder.Services.AddScoped<IStockAdjustmentRepository, StockAdjustmentRepository>();
builder.Services.AddScoped<IDeliveryOrderRepository, DeliveryOrderRepository>();
builder.Services.AddScoped<ISalesInvoiceRepository, SalesInvoiceRepository>();
builder.Services.AddScoped<IPurchaseAlertRepository, PurchaseAlertRepository>();
builder.Services.AddScoped<IPurchaseGoodReceiptService, PurchaseGoodReceiptService>();
builder.Services.AddScoped<IIncotermsService, IncotermsService>();
builder.Services.AddScoped<IflagIssuesServices, FlagIssuesServices>();
builder.Services.AddScoped<ISupplierInvoicePinService, SupplierInvoicePinService>();
builder.Services.AddScoped<IPurchaseRequestTempService, PurchaseRequestTempService>();
builder.Services.AddScoped<ICatagoryService, CatagoryServices>();
builder.Services.AddScoped<IStockIssueServices, StockIssuesServices>();
builder.Services.AddScoped<IBinServices, BinServices>();
builder.Services.AddScoped<IStockAdjustmentServices, StockAdjustmentServices>();
builder.Services.AddScoped<IQuotationService, QuotationService>();
builder.Services.AddScoped<IDeliveryOrderService, DeliveryOrderService>();
builder.Services.AddScoped<IRunningNumberRepository, RunningNumberRepository>();
builder.Services.AddScoped<IPickingRepository, PickingRepository>();
builder.Services.AddScoped<IPickingService, PickingService>();
builder.Services.AddSingleton<ICodeImageService, CodeImageService>();
builder.Services.AddScoped<ISalesInvoiceService, SalesInvoiceService>();
builder.Services.AddScoped<IPurchaseAlertService, PurchaseAlertService>();
builder.Services.AddScoped<IArReceiptRepository, ArReceiptRepository>();
builder.Services.AddScoped<IArReceiptService, ArReceiptService>();

builder.Services.AddScoped<IJournalRepository, JournalRepository>();
builder.Services.AddScoped<IJournalService, JournalService>();

builder.Services.AddDataProtection();
builder.Services.AddSingleton<IMobileLinkTokenService, MobileLinkTokenService>();




builder.Services.AddHttpContextAccessor();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// CORS
const string CorsPolicy = "Frontend";
builder.Services.AddCors(options =>
{
    options.AddPolicy(name: CorsPolicy, policy =>
    {
        policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
    });
});
builder.Configuration
       .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
       .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", optional: true)
       .AddEnvironmentVariables();

// JWT
var jwtSecret = builder.Configuration["Jwt:Secret"];
if (!string.IsNullOrWhiteSpace(jwtSecret))
{
    var key = Encoding.ASCII.GetBytes(jwtSecret);
    builder.Services.AddAuthentication(options =>
    {
        options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
        options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
    })
    .AddJwtBearer(options =>
    {
        options.RequireHttpsMetadata = false;
        options.SaveToken = true;
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuerSigningKey = true,
            ValidateIssuer = false,
            ValidateAudience = false,
            IssuerSigningKey = new SymmetricSecurityKey(key),
        };
    });

    builder.Services.AddAuthorization();
}
builder.Services.AddSwaggerGen(c =>
{
    c.OperationFilter<FileUploadOperationFilter>();
});
// PORT binding
var port = Environment.GetEnvironmentVariable("PORT");
if (!string.IsNullOrEmpty(port))
{
    builder.WebHost.UseUrls($"http://*:{port}");
}

builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(7182));

var app = builder.Build();

// ===================== Middleware =====================

if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
    app.UseSwagger();
    app.UseSwaggerUI();
}
app.Use(async (ctx, next) =>
{
    // Only protect SPA HTML pages (NOT APIs, NOT static files)
    var accept = ctx.Request.Headers.Accept.ToString();

    var isHtmlGet =
        ctx.Request.Method.Equals("GET", StringComparison.OrdinalIgnoreCase) &&
        (accept.Contains("text/html", StringComparison.OrdinalIgnoreCase) ||
         ctx.Request.Path.Value?.EndsWith(".html", StringComparison.OrdinalIgnoreCase) == true);

    // ✅ Allow APIs always
    if (ctx.Request.Path.StartsWithSegments("/api") ||
        ctx.Request.Path.StartsWithSegments("/swagger"))
    {
        await next();
        return;
    }

    // ✅ Allow static files always (css/js/images/fonts)
    if (ctx.Request.Path.StartsWithSegments("/assets") ||
        ctx.Request.Path.StartsWithSegments("/images") ||
        ctx.Request.Path.StartsWithSegments("/fonts") ||
        ctx.Request.Path.StartsWithSegments("/css") ||
        ctx.Request.Path.StartsWithSegments("/js") ||
        ctx.Request.Path.StartsWithSegments("/lib"))
    {
        await next();
        return;
    }

    // If not an HTML page request, don't block
    if (!isHtmlGet)
    {
        await next();
        return;
    }

    // ✅ Allow normal SPA pages (index.html + angular routes)
    // But protect ONLY the token pages below.

    // 🔒 Protect: mobile receiving route with token
    if (ctx.Request.Path.StartsWithSegments("/purchase/mobilereceiving"))
    {
        var poNo = ctx.Request.Query["poNo"].ToString();
        var t = ctx.Request.Query["t"].ToString();

        var tokenSvc = ctx.RequestServices.GetRequiredService<IMobileLinkTokenService>();

        if (!tokenSvc.TryValidate(t, poNo, out var err))
        {
            ctx.Response.StatusCode = 403;
            await ctx.Response.WriteAsync("Access denied: " + err);
            return;
        }

        await next();
        return;
    }

    // 🔒 Protect: picking scan page with token
    if (ctx.Request.Path.StartsWithSegments("/scan/so.html"))
    {
        var soId = ctx.Request.Query["id"].ToString();
        var t = ctx.Request.Query["t"].ToString();

        var tokenSvc = ctx.RequestServices.GetRequiredService<IMobileLinkTokenService>();

        if (!tokenSvc.TryValidate(t, soId, out var err))
        {
            ctx.Response.StatusCode = 403;
            await ctx.Response.WriteAsync("Access denied: " + err);
            return;
        }

        await next();
        return;
    }

    // ✅ Everything else HTML allowed
    await next();
});

app.UseStaticFiles();

if (resolvedWebRoot == vuexyPath && Directory.Exists(plainWwwroot))
{
    app.UseStaticFiles(new StaticFileOptions
    {
        FileProvider = new PhysicalFileProvider(plainWwwroot),
        RequestPath = ""
    });
}

app.UseRouting();
app.UseCors(CorsPolicy);
app.UseAuthentication();
// app.UseAuthorization();

app.MapControllers();
app.MapFallbackToFile("index.html");

// ===================== FluentScheduler – Recurring Journal Job =====================

var scopeFactory = app.Services.GetRequiredService<IServiceScopeFactory>();

// Registry-based configuration – this is what was missing before
JobManager.Initialize(new RecurringJournalRegistry(scopeFactory));

app.Run();


// ===================== Registry class =====================

public class RecurringJournalRegistry : Registry
{
    public RecurringJournalRegistry(IServiceScopeFactory scopeFactory)
    {
        Schedule(() =>
        {
            using var scope = scopeFactory.CreateScope();
            var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
            var journalService = scope.ServiceProvider.GetRequiredService<IJournalService>();

            try
            {
                var defaultTimezone = "Asia/Kolkata";
                var tz = TimeZoneInfo.FindSystemTimeZoneById(defaultTimezone);
                var nowLocal = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tz);

                logger.LogInformation("⏱ Recurring journal job fired at {time} ({tz})", nowLocal, defaultTimezone);

                // block the async call so scheduler waits for it
                var processed = journalService
                    .ProcessRecurringAsync(nowLocal, defaultTimezone)
                    .GetAwaiter()
                    .GetResult();

                logger.LogInformation("✅ Recurring journal job processed {count} template(s)", processed);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in recurring Journal job");
            }

        }).ToRunEvery(1).Days();  // run every 1 minute
    }
}
