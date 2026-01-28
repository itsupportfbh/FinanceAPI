using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using System.Data;

namespace FinanceApi.Repositories
{
    public class PeriodCloseRepository : DynamicRepository, IPeriodCloseRepository
    {
        public PeriodCloseRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        // ---------- PERIOD LIST / STATUS / LOCK / FX ----------

        public async Task<IEnumerable<PeriodOptionDto>> GetPeriodsAsync()
        {
            return await Connection.QueryAsync<PeriodOptionDto>(
                "dbo.sp_PeriodClose_GetPeriods",
                commandType: CommandType.StoredProcedure);
        }

        public async Task<PeriodStatusDto?> GetStatusAsync(int periodId)
        {
            return await Connection.QueryFirstOrDefaultAsync<PeriodStatusDto>(
                "dbo.sp_PeriodClose_GetStatus",
                new { PeriodId = periodId },
                commandType: CommandType.StoredProcedure);
        }

        public async Task<PeriodStatusDto> SetLockAsync(int periodId, bool lockFlag, int userId)
        {
            return await Connection.QuerySingleAsync<PeriodStatusDto>(
                "dbo.sp_PeriodClose_SetLock",
                new { PeriodId = periodId, Lock = lockFlag, UserId = userId },
                commandType: CommandType.StoredProcedure);
        }

        public async Task<int> RunFxRevalAsync(FxRevalRequestDto req, int userId)
        {
            return await Connection.ExecuteScalarAsync<int>(
                "dbo.sp_PeriodClose_RunFxReval",
                new
                {
                    PeriodId = req.PeriodId,
                    FxDate = req.FxDate,
                    UserId = userId
                },
                commandType: CommandType.StoredProcedure);
        }


        // ---------- PERIOD BY DATE (helper) ----------

        public async Task<AccountingPeriod?> GetByDateAsync(DateTime date)
        {
            const string sql = @"
SELECT TOP (1)
    Id,
    PeriodCode,
    PeriodName,
    StartDate,
    EndDate,
    IsLocked,
    LockDate,
    LockedBy,
    IsActive,
    CreatedBy,
    CreatedDate,
    UpdatedBy,
    UpdatedDate
FROM dbo.AccountingPeriod
WHERE @Date BETWEEN StartDate AND EndDate
  AND IsActive = 1
ORDER BY StartDate;";

            // Connection property already gives you an openable connection,
            // no need for 'using var conn = Connection;'
            return await Connection.QueryFirstOrDefaultAsync<AccountingPeriod>(
                sql,
                new { Date = date.Date });
        }

        // ---------- VALIDATION: ensure transaction date is in an open period ----------

        public async Task EnsureOpenAsync(DateTime transDate)
        {
            // Automatically adjust date if it's before SQL Server datetime min
            if (transDate < (DateTime)System.Data.SqlTypes.SqlDateTime.MinValue)
            {
                transDate = System.Data.SqlTypes.SqlDateTime.MinValue.Value;
            }

            var period = await GetByDateAsync(transDate);

            if (period == null)
            {
                throw new InvalidOperationException(
                    $"No accounting period defined for date {transDate:dd-MMM-yyyy}. " +
                    "Please create/open the period first.");
            }

            if (period.IsLocked)
            {
                var name = string.IsNullOrWhiteSpace(period.PeriodName)
                    ? period.StartDate.ToString("MMM yyyy")
                    : period.PeriodName;

                throw new InvalidOperationException(
                    $"Accounting period '{name}' is locked. " +
                    "You cannot post this transaction to that period.");
            }
        }



    }
}
