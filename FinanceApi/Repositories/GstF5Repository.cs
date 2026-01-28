using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using System.Data;

namespace FinanceApi.Repositories
{
    public class GstF5Repository : DynamicRepository, IGstF5Repository
    {
        public GstF5Repository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<GstFinancialYearOptionDto>> GetFinancialYearsAsync()
        {
            return await Connection.QueryAsync<GstFinancialYearOptionDto>(
                "dbo.sp_GstFinancialYears",
                commandType: CommandType.StoredProcedure);
        }

        public async Task<IEnumerable<GstPeriodOptionDto>> GetPeriodsByYearAsync(int fyStartYear)
        {
            return await Connection.QueryAsync<GstPeriodOptionDto>(
                "dbo.sp_GstPeriodsByFinancialYear",
                new { FyStartYear = fyStartYear },
                commandType: CommandType.StoredProcedure);
        }

        /// <summary>
        /// Get GST return for a period.
        /// - SystemSummary is always from sp_GstSystemSummaryByPeriod
        /// - Box6/Box7 are NEVER auto-filled from system.
        ///   For a brand new period they start at 0.
        /// </summary>
        public async Task<GstReturnDto> GetReturnForPeriodAsync(int periodId, int userId)
        {
            // 1) System summary from SP (always)
            var sys = await Connection.QuerySingleAsync<GstSystemSummaryDto>(
                "dbo.sp_GstSystemSummaryByPeriod",
                new { PeriodId = periodId },
                commandType: CommandType.StoredProcedure);

            // 2) Try to get existing saved F5 return for this period
            const string sqlReturn = @"
SELECT TOP (1)
    r.Id,
    r.PeriodId,
    r.Box6OutputTax,
    r.Box7InputTax,
    (r.Box6OutputTax - r.Box7InputTax) AS Box8NetPayable,
    CASE WHEN r.Status = 1 THEN 'LOCKED' ELSE 'OPEN' END AS [Status]
FROM dbo.GstReturn r
WHERE r.IsActive = 1
  AND r.PeriodId = @PeriodId
ORDER BY r.Id DESC;";

            var row = await Connection.QueryFirstOrDefaultAsync<GstReturnDto>(
                sqlReturn,
                new { PeriodId = periodId });

            if (row == null)
            {
                // Brand-new period:
                // F5 boxes are fully manual, so default them to 0, not system values.
                row = new GstReturnDto
                {
                    Id = 0,
                    PeriodId = periodId,
                    Box6OutputTax = 0m,
                    Box7InputTax = 0m,
                    Box8NetPayable = 0m,
                    Status = "OPEN"
                };
            }

            // Always attach system summary so UI can compare
            row.SystemSummary = sys;
            return row;
        }

        public async Task<GstReturnDto> ApplyAndLockAsync(GstApplyLockRequest req, int userId)
        {
            // User manually decides Box6/Box7; we just save them.
            const string sqlUpsert = @"
DECLARE @ExistingId INT;

SELECT @ExistingId = Id
FROM dbo.GstReturn
WHERE PeriodId = @PeriodId
  AND IsActive = 1;

IF @ExistingId IS NULL
BEGIN
    INSERT INTO dbo.GstReturn
        (PeriodId, Box6OutputTax, Box7InputTax, Status, IsActive, CreatedBy, CreatedDate)
    VALUES
        (@PeriodId, @Box6OutputTax, @Box7InputTax, 1, 1, @UserId, SYSDATETIME());

    SET @ExistingId = SCOPE_IDENTITY();
END
ELSE
BEGIN
    UPDATE dbo.GstReturn
    SET Box6OutputTax = @Box6OutputTax,
        Box7InputTax = @Box7InputTax,
        Status       = 1,
        UpdatedBy    = @UserId,
        UpdatedDate  = SYSDATETIME()
    WHERE Id = @ExistingId;
END

SELECT
    r.Id,
    r.PeriodId,
    r.Box6OutputTax,
    r.Box7InputTax,
    (r.Box6OutputTax - r.Box7InputTax) AS Box8NetPayable,
    CASE WHEN r.Status = 1 THEN 'LOCKED' ELSE 'OPEN' END AS [Status]
FROM dbo.GstReturn r
WHERE r.Id = @ExistingId;";

            var dto = await Connection.QuerySingleAsync<GstReturnDto>(sqlUpsert, new
            {
                req.PeriodId,
                req.Box6OutputTax,
                req.Box7InputTax,
                UserId = userId
            });

            // Refresh system summary for the response
            var sys = await Connection.QuerySingleAsync<GstSystemSummaryDto>(
                "dbo.sp_GstSystemSummaryByPeriod",
                new { PeriodId = req.PeriodId },
                commandType: CommandType.StoredProcedure);

            dto.SystemSummary = sys;
            return dto;
        }

        public async Task<IEnumerable<GstAdjustmentDto>> GetAdjustmentsAsync(int periodId)
        {
            const string sql = @"
SELECT
    Id,
    PeriodId,
    LineType,
    Amount,
    ISNULL(Description,'') AS Description
FROM dbo.GstAdjustment
WHERE IsActive = 1
  AND PeriodId = @PeriodId
ORDER BY Id;";

            return await Connection.QueryAsync<GstAdjustmentDto>(sql, new { PeriodId = periodId });
        }

        public async Task<GstAdjustmentDto> SaveAdjustmentAsync(GstAdjustmentDto dto, int userId)
        {
            const string sql = @"
IF @Id IS NULL OR @Id = 0
BEGIN
    INSERT INTO dbo.GstAdjustment
        (PeriodId, LineType, Amount, Description, IsActive, CreatedBy, CreatedDate)
    VALUES
        (@PeriodId, @LineType, @Amount, @Description, 1, @UserId, SYSUTCDATETIME());

    SET @Id = SCOPE_IDENTITY();
END
ELSE
BEGIN
    UPDATE dbo.GstAdjustment
    SET LineType   = @LineType,
        Amount     = @Amount,
        Description= @Description,
        UpdatedBy  = @UserId,
        UpdatedDate= SYSUTCDATETIME()
    WHERE Id       = @Id;
END

SELECT
    Id,
    PeriodId,
    LineType,
    Amount,
    ISNULL(Description,'') AS Description
FROM dbo.GstAdjustment
WHERE Id = @Id;";

            var param = new
            {
                dto.Id,
                dto.PeriodId,
                dto.LineType,
                dto.Amount,
                dto.Description,
                UserId = userId
            };

            return await Connection.QuerySingleAsync<GstAdjustmentDto>(sql, param);
        }

        public async Task DeleteAdjustmentAsync(int id, int userId)
        {
            const string sql = @"
UPDATE dbo.GstAdjustment
SET IsActive   = 0,
    UpdatedBy  = @UserId,
    UpdatedDate= SYSUTCDATETIME()
WHERE Id = @Id;";

            await Connection.ExecuteAsync(sql, new { Id = id, UserId = userId });
        }

        public async Task<IEnumerable<GstDocRowDto>> GetDocsByPeriodAsync(int periodId)
        {
            return await Connection.QueryAsync<GstDocRowDto>(
                "dbo.sp_GstSystemDocsByPeriod",
                new { PeriodId = periodId },
                commandType: CommandType.StoredProcedure);
        }
        public async Task<IEnumerable<GstDetailRowDto>> GetGstDetailsAsync(
    DateTime startDate,
    DateTime endDate,
    string? docType,
    string? partySearch)
        {
            return await Connection.QueryAsync<GstDetailRowDto>(
                "dbo.sp_GstDetails",
                new
                {
                    StartDate = startDate.Date,
                    EndDate = endDate.Date,
                    DocType = string.IsNullOrWhiteSpace(docType) ? null : docType,
                    PartySearch = string.IsNullOrWhiteSpace(partySearch) ? null : partySearch
                },
                commandType: CommandType.StoredProcedure);
        }

    }
}
