using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using Microsoft.Data.SqlClient;
using UnityWorksERP.Finance.AR;

namespace FinanceApi.Repositories
{
    public class ArReceiptRepository : DynamicRepository, IArReceiptRepository
    {
        public ArReceiptRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        #region LIST / DETAIL / OPEN INVOICES

        public async Task<IEnumerable<ArReceiptListDto>> GetListAsync()
        {
            const string sql = @"
SELECT
    r.Id,
    r.ReceiptNo,
    r.CustomerId,
    ISNULL(c.CustomerName, '') AS CustomerName,
    r.ReceiptDate,
    r.PaymentMode,
    r.BankId,
    r.AmountReceived,
    r.TotalAllocated,
    r.Unallocated,
    r.Status,

    -- NEW: comma-separated list of invoice numbers for this receipt
    InvoiceNos =
    STUFF(
        (
            SELECT DISTINCT
                   ', ' + ISNULL(si.InvoiceNo,'')
            FROM dbo.ArReceiptAllocation ra
            INNER JOIN dbo.SalesInvoice si
                ON si.Id = ra.InvoiceId
            WHERE ra.ReceiptId = r.Id
              AND ra.IsActive = 1
        FOR XML PATH(''), TYPE
        ).value('.','nvarchar(max)')
        ,1,2,''
    )
FROM dbo.ArReceipt r
LEFT JOIN dbo.Customer c ON c.Id = r.CustomerId
WHERE r.IsActive = 1
ORDER BY r.ReceiptDate DESC, r.Id DESC;";

            return await Connection.QueryAsync<ArReceiptListDto>(sql);
        }


        public async Task<ArReceiptDetailDto?> GetByIdAsync(int id)
        {
            const string hdrSql = @"
SELECT
    r.Id,
    r.ReceiptNo,
    r.CustomerId,
    ISNULL(c.CustomerName, '') AS CustomerName,
    r.ReceiptDate,
    r.PaymentMode,
    r.BankId,
    r.AmountReceived,
    r.TotalAllocated,
    r.Unallocated,
    r.ReferenceNo,
    r.Remarks,
    r.Status
FROM dbo.ArReceipt r
LEFT JOIN dbo.Customer c ON c.Id = r.CustomerId
WHERE r.Id = @Id AND r.IsActive = 1;";

            const string lineSql = @"
SELECT
    ra.Id,
    ra.InvoiceId,
    ISNULL(v.InvoiceNo, '')      AS InvoiceNo,
    v.InvoiceDate                AS InvoiceDate,
    ISNULL(v.Amount,0)           AS Amount,      -- SI total
    ISNULL(v.PaidAmount,0)       AS PaidAmount,  -- all receipts + CN
    ISNULL(v.Balance,0)          AS Balance,     -- open AFTER this receipt
    ra.AllocatedAmount           AS AllocatedAmount
FROM dbo.ArReceiptAllocation ra
INNER JOIN dbo.vwSalesInvoiceOpenForReceipt v
        ON v.Id = ra.InvoiceId
WHERE ra.ReceiptId = @Id
  AND ra.IsActive = 1
ORDER BY ra.Id;";

            using var multi = await Connection.QueryMultipleAsync(
                $"{hdrSql} {lineSql}", new { Id = id });

            var header = await multi.ReadFirstOrDefaultAsync<ArReceiptDetailDto>();
            if (header == null) return null;

            header.Allocations = (await multi.ReadAsync<ArReceiptAllocationDto>()).ToList();
            return header;
        }


        public async Task<IEnumerable<SalesInvoiceOpenDto>> GetOpenInvoicesAsync(int customerId)
        {
            const string sql = @"
SELECT
    Id,
    InvoiceNo,
    InvoiceDate,
    Amount,
    PaidAmount,
    Balance
FROM dbo.vwSalesInvoiceOpenForReceipt
WHERE CustomerId = @CustomerId
  AND Balance > 0          -- only invoices with something still due
ORDER BY InvoiceDate;";

            return await Connection.QueryAsync<SalesInvoiceOpenDto>(
                sql,
                new { CustomerId = customerId });
        }




        #endregion

        #region CREATE / UPDATE / DELETE

        public async Task<int> CreateAsync(ArReceiptCreateUpdateDto dto, int userId)
        {
            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();

            try
            {
                // 1) Generate next receipt number
                const string nextNoSql = @"
DECLARE @Next INT = (SELECT ISNULL(MAX(Id),0) + 1 FROM dbo.ArReceipt);
SELECT 'RC-' + FORMAT(@Next, '000000');";

                var receiptNo = await conn.ExecuteScalarAsync<string>(
                    nextNoSql,
                    transaction: tx);

                var totalAllocated = dto.Allocations.Sum(a => a.AllocatedAmount);
                var unallocated = dto.AmountReceived - totalAllocated;

                // 2) Insert header
                const string insertHdrSql = @"
INSERT INTO dbo.ArReceipt
(
    ReceiptNo, CustomerId, ReceiptDate,
    PaymentMode, BankId,
    AmountReceived, TotalAllocated, Unallocated,
    ReferenceNo, Remarks,
    Status, IsActive,
    CreatedBy
)
VALUES
(
    @ReceiptNo, @CustomerId, @ReceiptDate,
    @PaymentMode, @BankId,
    @AmountReceived, @TotalAllocated, @Unallocated,
    @ReferenceNo, @Remarks,
    'Posted', 1,
    @CreatedBy
);
SELECT CAST(SCOPE_IDENTITY() AS INT);";

                var receiptId = await conn.ExecuteScalarAsync<int>(
                    insertHdrSql,
                    new
                    {
                        ReceiptNo = receiptNo,
                        dto.CustomerId,
                        dto.ReceiptDate,
                        dto.PaymentMode,
                        dto.BankId,
                        dto.AmountReceived,
                        TotalAllocated = totalAllocated,
                        Unallocated = unallocated,
                        dto.ReferenceNo,
                        dto.Remarks,
                        CreatedBy = userId
                    },
                    transaction: tx);

                // 3) Insert allocations
                const string insertLineSql = @"
INSERT INTO dbo.ArReceiptAllocation
(
    ReceiptId, InvoiceId, AllocatedAmount,
    IsActive, CreatedBy
)
VALUES
(
    @ReceiptId, @InvoiceId, @AllocatedAmount,
    1, @CreatedBy
);";

                foreach (var line in dto.Allocations.Where(a => a.AllocatedAmount > 0))
                {
                    await conn.ExecuteAsync(
                        insertLineSql,
                        new
                        {
                            ReceiptId = receiptId,
                            line.InvoiceId,
                            line.AllocatedAmount,
                            CreatedBy = userId
                        },
                        transaction: tx);
                }

                // 4) Post to GL
                await conn.ExecuteAsync(
                    "dbo.sp_PostArReceiptToGl",
                    new { ReceiptId = receiptId, UserId = userId },
                    transaction: tx,
                    commandType: CommandType.StoredProcedure);

                // 5) Update AccountBalance for BANK (if needed)
                if (dto.PaymentMode == "BANK" && dto.BankId.HasValue && dto.BankId.Value > 0)
                {
                    var bankHeadId = await GetBankBudgetLineIdAsync(conn, tx, dto.BankId.Value);
                    if (bankHeadId.HasValue)
                    {
                        // Money is coming INTO bank, so balance increases.
                        // You can choose AmountReceived or TotalAllocated – I use AmountReceived here.
                        await UpsertAccountBalanceAsync(
                            conn,
                            tx,
                            headId: bankHeadId.Value,
                            deltaAmount: dto.AmountReceived);
                    }
                }

                tx.Commit();
                return receiptId;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }


        public async Task UpdateAsync(ArReceiptCreateUpdateDto dto, int userId)
        {
            if (!dto.Id.HasValue)
                throw new ArgumentException("Id is required for update.", nameof(dto));

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();

            try
            {
                // -------------------------------------------------
                // 1) Load old header info (old BankId & TotalAllocated)
                // -------------------------------------------------
                const string getOldHdrSql = @"
SELECT BankId,
       ISNULL(TotalAllocated, 0.0) AS TotalAllocated
FROM dbo.ArReceipt
WHERE Id = @Id AND IsActive = 1;";

                var oldHdr = await conn.QuerySingleAsync<dynamic>(
                    getOldHdrSql,
                    new { Id = dto.Id.Value },
                    transaction: tx);

                int oldBankId = (int)oldHdr.BankId;
                decimal oldTotalAllocated = (decimal)oldHdr.TotalAllocated;

                // -------------------------------------------------
                // 2) New totals from DTO
                // -------------------------------------------------
                var totalAllocated = dto.Allocations.Sum(a => a.AllocatedAmount);
                var unallocated = dto.AmountReceived - totalAllocated;

                const string updateHdrSql = @"
UPDATE dbo.ArReceipt
SET CustomerId     = @CustomerId,
    ReceiptDate    = @ReceiptDate,
    PaymentMode    = @PaymentMode,
    BankId         = @BankId,
    AmountReceived = @AmountReceived,
    TotalAllocated = @TotalAllocated,
    Unallocated    = @Unallocated,
    ReferenceNo    = @ReferenceNo,
    Remarks        = @Remarks,
    UpdatedBy      = @UpdatedBy,
    UpdatedDate    = SYSUTCDATETIME()
WHERE Id = @Id AND IsActive = 1;";

                await conn.ExecuteAsync(
                    updateHdrSql,
                    new
                    {
                        dto.Id,
                        dto.CustomerId,
                        dto.ReceiptDate,
                        dto.PaymentMode,
                        dto.BankId,
                        dto.AmountReceived,
                        TotalAllocated = totalAllocated,
                        Unallocated = unallocated,
                        dto.ReferenceNo,
                        dto.Remarks,
                        UpdatedBy = userId
                    },
                    transaction: tx);

                // -------------------------------------------------
                // 3) Delete existing lines & re-insert
                // -------------------------------------------------
                const string deleteLinesSql = @"
DELETE FROM dbo.ArReceiptAllocation WHERE ReceiptId = @ReceiptId;";

                await conn.ExecuteAsync(
                    deleteLinesSql,
                    new { ReceiptId = dto.Id.Value },
                    transaction: tx);

                const string insertLineSql = @"
INSERT INTO dbo.ArReceiptAllocation
(
    ReceiptId, InvoiceId, AllocatedAmount,
    IsActive, CreatedBy
)
VALUES
(
    @ReceiptId, @InvoiceId, @AllocatedAmount,
    1, @CreatedBy
);";

                foreach (var line in dto.Allocations.Where(a => a.AllocatedAmount > 0))
                {
                    await conn.ExecuteAsync(
                        insertLineSql,
                        new
                        {
                            ReceiptId = dto.Id.Value,
                            line.InvoiceId,
                            line.AllocatedAmount,
                            CreatedBy = userId
                        },
                        transaction: tx);
                }

                // -------------------------------------------------
                // 4) Adjust AccountBalance for BANK head(s)
                //    HeadId = Bank.BudgetLineId
                // -------------------------------------------------
                int? oldBankHeadId = null;
                int? newBankHeadId = null;

                if (oldBankId > 0)
                    oldBankHeadId = await GetBankBudgetLineIdAsync(conn, tx, oldBankId);

                if (dto.BankId.HasValue && dto.BankId.Value > 0)
                    newBankHeadId = await GetBankBudgetLineIdAsync(conn, tx, dto.BankId.Value);


                if (oldBankHeadId.HasValue &&
                    newBankHeadId.HasValue &&
                    oldBankHeadId.Value == newBankHeadId.Value)
                {
                    // Same bank (same BudgetLineId) – apply only the difference
                    var delta = totalAllocated - oldTotalAllocated;
                    await UpsertAccountBalanceAsync(
                        conn,
                        tx,
                        headId: oldBankHeadId.Value,
                        deltaAmount: delta);
                }
                else
                {
                    // Bank changed (or BudgetLine changed)
                    if (oldBankHeadId.HasValue)
                    {
                        // Reverse old effect
                        await UpsertAccountBalanceAsync(
                            conn,
                            tx,
                            headId: oldBankHeadId.Value,
                            deltaAmount: -oldTotalAllocated);
                    }

                    if (newBankHeadId.HasValue)
                    {
                        // Apply new effect
                        await UpsertAccountBalanceAsync(
                            conn,
                            tx,
                            headId: newBankHeadId.Value,
                            deltaAmount: totalAllocated);
                    }
                }

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }


        public async Task DeleteAsync(int id, int userId)
        {
            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();

            try
            {
                const string sql = @"
UPDATE dbo.ArReceipt
SET IsActive    = 0,
    UpdatedBy   = @UserId,
    UpdatedDate = SYSUTCDATETIME()
WHERE Id = @Id;

UPDATE dbo.ArReceiptAllocation
SET IsActive    = 0,
    UpdatedBy   = @UserId,
    UpdatedDate = SYSUTCDATETIME()
WHERE ReceiptId = @Id;";

                await conn.ExecuteAsync(sql, new { Id = id, UserId = userId }, tx);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ===============================================
        // Helpers for Bank -> AccountBalance
        // ===============================================

        // Get the COA Head (BudgetLineId) for the selected bank
        private async Task<int?> GetBankBudgetLineIdAsync(
            IDbConnection conn,
            IDbTransaction tx,
            int bankId)
        {
            const string sql = @"
SELECT BudgetLineId
FROM dbo.Bank
WHERE Id = @BankId
  AND IsActive = 1;";  // adjust IsActive column name if needed

            var headId = await conn.ExecuteScalarAsync<int?>(
                sql,
                new { BankId = bankId },
                transaction: tx);

            return headId;     // this goes into AccountBalance.HeadId
        }

        // Upsert / update AccountBalance for that head
        private async Task UpsertAccountBalanceAsync(
            IDbConnection conn,
            IDbTransaction tx,
            int headId,
            decimal deltaAmount)
        {
            if (deltaAmount == 0) return;

            const string sql = @"
IF EXISTS (SELECT 1 FROM dbo.AccountBalance WHERE HeadId = @HeadId)
BEGIN
    UPDATE dbo.AccountBalance
    SET
        AvailableBalance = ISNULL(AvailableBalance, 0) + @DeltaAmount,
        LastUpdated      = SYSUTCDATETIME(),
        UpdatedDate      = SYSUTCDATETIME()
    WHERE HeadId = @HeadId;
END
ELSE
BEGIN
    INSERT INTO dbo.AccountBalance
    (
        HeadId,
        PeriodDebit,
        PeriodCredit,
        LastUpdated,
        AvailableBalance,
        UpdatedDate
    )
    VALUES
    (
        @HeadId,
        0,
        0,
        SYSUTCDATETIME(),
        @DeltaAmount,
        SYSUTCDATETIME()
    );
END";

            await conn.ExecuteAsync(
                sql,
                new { HeadId = headId, DeltaAmount = deltaAmount },
                transaction: tx);
        }


        #endregion
    }

}
