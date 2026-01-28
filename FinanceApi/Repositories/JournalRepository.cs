using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.Data.SqlClient;
using System.Data;

namespace FinanceApi.Repositories
{
    public class JournalRepository : DynamicRepository, IJournalRepository
    {
        public JournalRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        #region LIST / DETAILS

        // List screen – one row per journal, with total debit/credit
        public async Task<IEnumerable<JournalsDTO>> GetAllAsync()
        {
            const string sql = @"
SELECT 
    mj.Id,
    mj.JournalNo,
    mj.JournalDate,
    SUM(mjl.Debit)  AS DebitAmount,
    SUM(mjl.Credit) AS CreditAmount,
    mj.IsRecurring,
    mj.RecurringFrequency,
    mj.IsPosted,
    mj.[Description]
FROM dbo.ManualJournal mj
INNER JOIN dbo.ManualJournalLine mjl
    ON mjl.JournalId = mj.Id
WHERE mj.IsActive    = 1
  AND mj.IsPosted    = 0          -- only not-posted journals
  AND mj.IsRecurring = 0
  AND mjl.IsActive   = 1
GROUP BY
    mj.Id,
    mj.JournalNo,
    mj.JournalDate,
    mj.IsRecurring,
    mj.RecurringFrequency,
    mj.IsPosted,
    mj.[Description];";

            return await Connection.QueryAsync<JournalsDTO>(sql);
        }

        public async Task<int> MarkAsPostedAsync(IEnumerable<int> ids)
        {
            const string sql = @"
UPDATE dbo.ManualJournal
SET 
    IsPosted    = 1,
    UpdatedDate = SYSUTCDATETIME()
WHERE Id IN @Ids
  AND IsActive    = 1
  AND IsPosted    = 0
  AND IsRecurring = 0;";

            return await Connection.ExecuteAsync(sql, new { Ids = ids });
        }

        #endregion

        #region CREATE

        public async Task<int> CreateAsync(ManualJournalCreateDto dto)
        {
            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await ((SqlConnection)conn).OpenAsync();

            using var tx = conn.BeginTransaction();

            // 1) Insert header (ManualJournal) with auto JournalNo
            const string insertHeaderSql = @"
DECLARE @NextNo INT;

SELECT 
    @NextNo = ISNULL(
        MAX(CAST(SUBSTRING(JournalNo, 4, 10) AS INT)), 
        0
    ) + 1
FROM dbo.ManualJournal WITH (UPDLOCK, HOLDLOCK);

DECLARE @NewJournalNo VARCHAR(20) =
    'JN-' + RIGHT('0000' + CAST(@NextNo AS VARCHAR(10)), 4);

INSERT INTO dbo.ManualJournal
(
    JournalNo,
    JournalDate,
    [Description],
    IsRecurring,
    RecurringFrequency,
    RecurringInterval,
    RecurringStartDate,
    RecurringEndType,
    RecurringEndDate,
    RecurringCount,
    ProcessedCount,
    NextRunDate,
    TimeZone,
    CreatedBy,
    CreatedDate,
    IsActive,
    IsPosted
)
VALUES
(
    @NewJournalNo,                -- auto generated JN-0001, JN-0002, ...
    @JournalDateUtc,
    @Description,
    @IsRecurring,
    @RecurringFrequency,
    @RecurringInterval,
    @RecurringStartDateUtc,
    @RecurringEndType,
    @RecurringEndDateUtc,
    @RecurringCount,
    0,
    CASE 
        WHEN @IsRecurring = 1 THEN
            COALESCE(@RecurringStartDateUtc, @JournalDateUtc)
        ELSE NULL
    END,
    @Timezone,
    @CreatedBy,
    SYSUTCDATETIME(),
    1,
    0               -- not posted yet, will be set by Post button
);

SELECT CAST(SCOPE_IDENTITY() AS INT);";

            const string insertLineSql = @"
INSERT INTO dbo.ManualJournalLine
(
    JournalId,
    AccountId,
    LineDescription,
    Debit,
    Credit,
    IsActive,
    CreatedBy,
    CreatedDate
)
VALUES
(
    @JournalId,
    @AccountId,
    @LineDescription,
    @Debit,
    @Credit,
    1,
    @CreatedBy,
    SYSUTCDATETIME()
);";

            try
            {
                // Insert header
                var journalId = await conn.ExecuteScalarAsync<int>(
                    insertHeaderSql,
                    dto,
                    transaction: tx);

                // Insert lines
                foreach (var line in dto.Lines.Where(l => l.AccountId > 0 && (l.Debit != 0 || l.Credit != 0)))
                {
                    var lineParams = new
                    {
                        JournalId = journalId,
                        AccountId = line.AccountId,
                        LineDescription = line.Description,
                        Debit = line.Debit,
                        Credit = line.Credit,
                        CreatedBy = dto.CreatedBy
                    };

                    await conn.ExecuteAsync(
                        insertLineSql,
                        lineParams,
                        transaction: tx);
                }

                // ❌ NO GL POSTING – just save header + lines
                // Posting will only set IsPosted=1 via MarkAsPostedAsync / Post button.

                tx.Commit();
                return journalId;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        #endregion

        #region RECURRING – LIST & DETAILS

        public async Task<IEnumerable<ManualJournalDto>> GetAllRecurringDetails()
        {
            const string sql = @"
SELECT
    mj.Id,
    mj.JournalNo,
    mj.JournalDate,
    mj.[Description],
    mj.IsRecurring,
    mj.RecurringFrequency,
    mj.RecurringInterval,
    mj.RecurringStartDate,
    mj.RecurringEndType,
    mj.RecurringEndDate,
    mj.RecurringCount,
    mj.ProcessedCount,
    mj.NextRunDate,
    mj.IsPosted,
    mj.TimeZone AS Timezone
FROM dbo.ManualJournal mj
WHERE mj.IsActive = 1
  AND mj.IsRecurring = 1
ORDER BY mj.JournalDate DESC, mj.Id DESC;";

            var headers = (await Connection.QueryAsync<ManualJournalDto>(sql)).ToList();
            return headers;
        }

        public async Task<ManualJournalDto?> GetByIdAsync(int id)
        {
            const string headerSql = @"
SELECT
    mj.Id,
    mj.JournalNo,
    mj.JournalDate,
    mj.[Description],
    mj.IsRecurring,
    mj.RecurringFrequency,
    mj.RecurringInterval,
    mj.RecurringStartDate,
    mj.RecurringEndType,
    mj.RecurringEndDate,
    mj.RecurringCount,
    mj.ProcessedCount,
    mj.NextRunDate,
    mj.IsPosted,
    mj.TimeZone AS Timezone
FROM dbo.ManualJournal mj
WHERE mj.Id = @Id AND mj.IsActive = 1;";

            const string linesSql = @"
SELECT
    mjl.Id,
    mjl.JournalId,
    mjl.AccountId,
    coa.HeadName AS AccountName,
    mjl.LineDescription,
    mjl.Debit,
    mjl.Credit
FROM dbo.ManualJournalLine mjl
INNER JOIN dbo.ChartOfAccount coa
    ON coa.Id = mjl.AccountId
WHERE mjl.JournalId = @Id
  AND mjl.IsActive = 1
ORDER BY mjl.Id;";

            using var multi = await Connection.QueryMultipleAsync(
                headerSql + "\n" + linesSql,
                new { Id = id });

            var header = await multi.ReadFirstOrDefaultAsync<ManualJournalDto>();
            if (header == null)
                return null;

            var lines = await multi.ReadAsync<ManualJournalLineDto>();
            header.Lines = lines;

            return header;
        }

        #endregion

        #region PROCESS RECURRING

        public async Task<int> ProcessRecurringAsync(DateTime processUtc)
        {
            // 1) Select recurring templates whose NextRunDate <= now
            const string selectSql = @"
SELECT
    mj.Id,
    mj.JournalNo,
    mj.JournalDate,
    mj.[Description],
    mj.IsRecurring,
    mj.RecurringFrequency,
    mj.RecurringInterval,
    mj.RecurringStartDate,
    mj.RecurringEndType,
    mj.RecurringEndDate,
    mj.RecurringCount,
    mj.ProcessedCount,
    mj.NextRunDate,
    mj.CreatedBy
FROM dbo.ManualJournal mj
WHERE mj.IsActive = 1
  AND mj.IsRecurring = 1
  AND mj.NextRunDate IS NOT NULL
  AND mj.NextRunDate <= @NowUtc;";

            const string insertHeaderSql = @"
INSERT INTO dbo.ManualJournal
(
    JournalNo,
    JournalDate,
    [Description],
    IsRecurring,
    RecurringFrequency,
    RecurringInterval,
    RecurringStartDate,
    RecurringEndType,
    RecurringEndDate,
    RecurringCount,
    ProcessedCount,
    NextRunDate,
    TimeZone,
    CreatedBy,
    CreatedDate,
    IsActive,
    IsPosted
)
VALUES
(
    NULL,            -- you can later generate number if needed
    @JournalDate,
    @Description,
    0,               -- new entry is not recurring
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    0,
    NULL,
    NULL,
    @CreatedBy,
    SYSUTCDATETIME(),
    1,
    1               -- mark as posted
);

SELECT CAST(SCOPE_IDENTITY() AS INT);";

            const string insertLineSql = @"
INSERT INTO dbo.ManualJournalLine
(
    JournalId,
    AccountId,
    LineDescription,
    Debit,
    Credit,
    IsActive,
    CreatedBy,
    CreatedDate
)
VALUES
(
    @JournalId,
    @AccountId,
    @LineDescription,
    @Debit,
    @Credit,
    1,
    @CreatedBy,
    SYSUTCDATETIME()
);";

            const string updateTemplateSql = @"
UPDATE dbo.ManualJournal
SET ProcessedCount = @ProcessedCount,
    NextRunDate    = @NextRunDate,
    UpdatedDate    = SYSUTCDATETIME()
WHERE Id = @Id;";

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                conn.Open();

            using var tran = conn.BeginTransaction();

            try
            {
                var templates = (await conn.QueryAsync<TemplateRow>(
                    selectSql,
                    new { NowUtc = processUtc },
                    tran)).ToList();

                if (templates.Count == 0)
                {
                    tran.Commit();
                    return 0;
                }

                int generatedCount = 0;

                foreach (var t in templates)
                {
                    if (!t.NextRunDate.HasValue)
                        continue;

                    // Load template lines
                    var templateLines = (await conn.QueryAsync<TemplateLineRow>(@"
SELECT
    AccountId,
    LineDescription,
    Debit,
    Credit
FROM dbo.ManualJournalLine
WHERE JournalId = @JournalId
  AND IsActive  = 1;",
                        new { JournalId = t.Id }, tran)).ToList();

                    if (templateLines.Count == 0)
                        continue;

                    // Insert new header (occurrence)
                    var newHeaderParams = new
                    {
                        JournalDate = t.NextRunDate.Value,
                        Description = t.Description,
                        CreatedBy = t.CreatedBy
                    };

                    var newJournalId = await conn.ExecuteScalarAsync<int>(
                        insertHeaderSql,
                        newHeaderParams,
                        tran);

                    // Insert lines
                    foreach (var line in templateLines)
                    {
                        var lineParams = new
                        {
                            JournalId = newJournalId,
                            AccountId = line.AccountId,
                            LineDescription = line.LineDescription,
                            Debit = line.Debit,
                            Credit = line.Credit,
                            CreatedBy = t.CreatedBy
                        };

                        await conn.ExecuteAsync(insertLineSql, lineParams, tran);
                    }

                    // ❌ No GL posting here either – just mark IsPosted = 1 on header (already set).

                    generatedCount++;

                    // Compute next run date
                    var currentNextRun = t.NextRunDate.Value;
                    var nextDate = GetNextRunDate(currentNextRun, t.RecurringFrequency, t.RecurringInterval);
                    var newProcessedCount = t.ProcessedCount + 1;

                    if (t.RecurringEndType == "EndByCount"
                        && t.RecurringCount.HasValue
                        && newProcessedCount >= t.RecurringCount.Value)
                    {
                        nextDate = null;
                    }

                    if (t.RecurringEndType == "EndByDate"
                        && t.RecurringEndDate.HasValue
                        && nextDate.HasValue
                        && nextDate.Value.Date > t.RecurringEndDate.Value.Date)
                    {
                        nextDate = null;
                    }

                    var updateParams = new
                    {
                        Id = t.Id,
                        ProcessedCount = newProcessedCount,
                        NextRunDate = (object?)nextDate
                    };

                    await conn.ExecuteAsync(updateTemplateSql, updateParams, tran);
                }

                tran.Commit();
                return generatedCount;
            }
            catch
            {
                tran.Rollback();
                throw;
            }
        }

        #endregion

        #region Helper classes

        private class TemplateRow
        {
            public int Id { get; set; }
            public string? JournalNo { get; set; }
            public DateTime JournalDate { get; set; }
            public string? Description { get; set; }
            public bool IsRecurring { get; set; }
            public string? RecurringFrequency { get; set; }
            public int? RecurringInterval { get; set; }
            public DateTime? RecurringStartDate { get; set; }
            public string? RecurringEndType { get; set; }
            public DateTime? RecurringEndDate { get; set; }
            public int? RecurringCount { get; set; }
            public int ProcessedCount { get; set; }
            public DateTime? NextRunDate { get; set; }
            public int? CreatedBy { get; set; }
        }

        private class TemplateLineRow
        {
            public int AccountId { get; set; }
            public string? LineDescription { get; set; }
            public decimal Debit { get; set; }
            public decimal Credit { get; set; }
        }

        private DateTime? GetNextRunDate(DateTime currentUtc, string? freq, int? interval)
        {
            if (string.IsNullOrEmpty(freq))
                return null;

            var step = (interval.HasValue && interval.Value > 0) ? interval.Value : 1;

            return freq switch
            {
                "Daily" => currentUtc.AddDays(step),
                "Weekly" => currentUtc.AddDays(7 * step),
                "Monthly" => currentUtc.AddMonths(step),
                "Quarterly" => currentUtc.AddMonths(3 * step),
                "Yearly" => currentUtc.AddYears(step),
                _ => (DateTime?)null
            };
        }

        #endregion
    }
}
