// Repositories/SupplierDebitNoteRepository.cs
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;

namespace FinanceApi.Repositories
{
    public class SupplierDebitNoteRepository : DynamicRepository, ISupplierDebitNoteRepository
    {
        public SupplierDebitNoteRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory) { }

        public async Task<IEnumerable<SupplierDebitNoteDTO>> GetAllAsync()
        {
            const string sql = @"
SELECT
    dn.Id,
    dn.DebitNoteNo,       -- NEW
    dn.ReferenceNo,
    dn.SupplierId,
    s.Name,
    dn.NoteDate,
    dn.Reason,
    dn.Amount,
    dn.Status,
    dn.LinesJson,
    dn.PinId,
    dn.GrnId
FROM dbo.SupplierDebitNote dn
LEFT JOIN dbo.Suppliers s ON s.Id = dn.SupplierId
WHERE dn.IsActive = 1
ORDER BY dn.Id DESC;";

            return await Connection.QueryAsync<SupplierDebitNoteDTO>(sql);
        }

        public async Task<SupplierDebitNoteDTO?> GetByIdAsync(int id)
        {
            const string sql = @"
SELECT
    dn.Id,
    dn.DebitNoteNo,       -- NEW
    dn.ReferenceNo,
    dn.SupplierId,
    s.Name,
    dn.NoteDate,
    dn.Reason,
    dn.Amount,
    dn.Status,
    dn.LinesJson,
    dn.PinId,
    dn.GrnId
FROM dbo.SupplierDebitNote dn
LEFT JOIN dbo.Suppliers s ON s.Id = dn.SupplierId
WHERE dn.Id = @Id;";

            return await Connection.QueryFirstOrDefaultAsync<SupplierDebitNoteDTO>(sql, new { Id = id });
        }


        public async Task<int> CreateAsync(SupplierDebitNote model)
        {
            // 1) Generate the next auto number
            model.DebitNoteNo = await GetNextDebitNoteNoAsync();

            const string sql = @"
INSERT INTO dbo.SupplierDebitNote
(
    DebitNoteNo,              -- 👈 NEW COLUMN
    SupplierId,
    PinId,
    GrnId,
    ReferenceNo,
    Reason,
    NoteDate,
    Amount,
    LinesJson,
    Status,
    IsActive,
    CreatedBy,
    CreatedDate
)
VALUES
(
    @DebitNoteNo,             -- 👈 USE GENERATED VALUE
    @SupplierId,
    @PinId,
    @GrnId,
    @ReferenceNo,
    @Reason,
    @NoteDate,
    @Amount,
    @LinesJson,
    @Status,
    1,
    @CreatedBy,
    SYSDATETIME()
);
SELECT CAST(SCOPE_IDENTITY() AS INT);";

            return await Connection.ExecuteScalarAsync<int>(sql, model);
        }

        public async Task UpdateAsync(SupplierDebitNote model)
        {
            const string sql = @"
UPDATE dbo.SupplierDebitNote
SET
    SupplierId  = @SupplierId,
    PinId       = @PinId,
    GrnId       = @GrnId,
    ReferenceNo = @ReferenceNo,
    Reason      = @Reason,
    NoteDate    = @NoteDate,
    Amount      = @Amount,
    LinesJson   = @LinesJson,
    Status      = @Status,
    UpdatedBy   = @UpdatedBy,
    UpdatedDate = SYSDATETIME()
WHERE Id = @Id;";

            await Connection.ExecuteAsync(sql, model);
        }

        public async Task DeleteAsync(int id)
        {
            const string sql = @"
UPDATE dbo.SupplierDebitNote
SET IsActive = 0,
    UpdatedDate = SYSDATETIME()
WHERE Id = @Id;";

            await Connection.ExecuteAsync(sql, new { Id = id });
        }
        public async Task<dynamic?> GetDebitNoteSourceAsync(int pinId)
        {
            const string sql = @"
SELECT TOP 1
    sip.Id          AS PinId,
    sip.InvoiceNo   AS PinNo,
    sip.LinesJson,
    gr.Id           AS GrnId,
    gr.GrnNo,
    po.Id           AS PoId,
    po.PurchaseOrderNo AS PoNo,
    po.SupplierId,
    s.Name
FROM dbo.SupplierInvoicePin sip
LEFT JOIN dbo.PurchaseGoodReceipt gr ON gr.Id = sip.GrnId
LEFT JOIN dbo.PurchaseOrder       po ON po.Id = gr.POID
LEFT JOIN dbo.Suppliers           s  ON s.Id = po.SupplierId
WHERE sip.Id = @PinId;";

            return await Connection.QueryFirstOrDefaultAsync<dynamic>(sql, new { PinId = pinId });
        }
        public async Task MarkDebitNoteAsync(int pinId, string userName)
        {
            const string sql = @"
UPDATE dbo.SupplierInvoicePin
SET Status     = 2,           -- 2 = Debit Note Created
    UpdatedBy  = @UserName,
    UpdatedDate = SYSDATETIME()
WHERE Id = @PinId;";

            await Connection.ExecuteAsync(sql, new { PinId = pinId, UserName = userName });
        }


        private async Task<string> GetNextDebitNoteNoAsync()
        {
            const string sql = @"
DECLARE @NextNo INT;

SELECT @NextNo =
    ISNULL(MAX(CAST(SUBSTRING(DebitNoteNo, 3, 20) AS INT)), 0) + 1
FROM dbo.SupplierDebitNote
WHERE DebitNoteNo LIKE 'DN%';

SELECT 'DN' + RIGHT('00000' + CAST(@NextNo AS VARCHAR(10)), 5);";

            var nextNo = await Connection.ExecuteScalarAsync<string>(sql);
            return nextNo ?? "DN00001";
        }

    }
}
