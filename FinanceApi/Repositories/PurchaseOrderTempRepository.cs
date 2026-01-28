using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.Data.SqlClient;

namespace FinanceApi.Repositories
{
    public class PurchaseOrderTempRepository : DynamicRepository, IPurchaseOrderTempRepository
    {
        private readonly IDbConnectionFactory _factory;
        public PurchaseOrderTempRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory) {

            _factory = connectionFactory;
        }

        public async Task<IEnumerable<PurchaseOrderTempDto>> GetDraftsAsync(string? createdBy = null)
        {
            var sql = @"
SELECT
  t.Id,
  t.PurchaseOrderNo,
  t.ApprovalStatus,
  t.SupplierId,
  s.Name        AS SupplierName,
  t.ApproveLevelId,
  al.Name       AS ApproveLevelName,
  t.PaymentTermId,
  pt.PaymentTermsName AS PaymentTermName,
  t.CurrencyId,
  cur.CurrencyName    AS CurrencyName,
  t.IncotermsId,
  inc.IncotermsName   AS IncotermsName,
  t.PoDate,
  t.DeliveryDate,
  t.Remarks,
  t.FxRate,
  t.Tax,
  t.Shipping,
  t.Discount,
  t.SubTotal,
  t.NetTotal,
  t.PoLines,
  t.CreatedBy,
  t.CreatedDate,
  t.UpdatedBy,
  t.UpdatedDate,
  t.IsActive
FROM dbo.PurchaseOrderTemp t
LEFT JOIN Suppliers      s   ON s.Id = t.SupplierId
LEFT JOIN ApprovalLevel  al  ON al.Id = t.ApproveLevelId
LEFT JOIN PaymentTerms   pt  ON pt.Id = t.PaymentTermId
LEFT JOIN Currency       cur ON cur.Id = t.CurrencyId
LEFT JOIN Incoterms      inc ON inc.Id = t.IncotermsId
WHERE t.IsActive = 1
" + (string.IsNullOrWhiteSpace(createdBy) ? "" : " AND t.CreatedBy = @createdBy ") + @"
ORDER BY t.CreatedDate DESC;";

            var rows = await Connection.QueryAsync<PurchaseOrderTempDto>(sql, new { createdBy });
            return rows.ToList();
        }

        public async Task<PurchaseOrderTemp?> GetByIdAsync(int id)
        {
            const string sql = @"SELECT * FROM dbo.PurchaseOrderTemp WHERE Id=@id AND IsActive=1;";
            return await Connection.QueryFirstOrDefaultAsync<PurchaseOrderTemp>(sql, new { id });
        }

        public async Task<int> CreateAsync(PurchaseOrderTemp draft)
        {
            const string sql = @"
INSERT INTO dbo.PurchaseOrderTemp
(PurchaseOrderNo, ApprovalStatus, SupplierId, ApproveLevelId, PaymentTermId, CurrencyId, IncotermsId,
 PoDate, DeliveryDate, Remarks, FxRate, Tax, Shipping, Discount, SubTotal, NetTotal, PoLines,
 CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
OUTPUT INSERTED.Id
VALUES
(@PurchaseOrderNo, @ApprovalStatus, @SupplierId, @ApproveLevelId, @PaymentTermId, @CurrencyId, @IncotermsId,
 @PoDate, @DeliveryDate, @Remarks, @FxRate, @Tax, @Shipping, @Discount, @SubTotal, @NetTotal, @PoLines,
 @CreatedBy, SYSUTCDATETIME(), @UpdatedBy, @UpdatedDate, 1);";
            return await Connection.ExecuteScalarAsync<int>(sql, draft);
        }

        public async Task UpdateAsync(PurchaseOrderTemp draft)
        {
            draft.UpdatedDate = DateTime.UtcNow;
            const string sql = @"
UPDATE dbo.PurchaseOrderTemp SET
  PurchaseOrderNo=@PurchaseOrderNo,
  ApprovalStatus=@ApprovalStatus,
  SupplierId=@SupplierId,
  ApproveLevelId=@ApproveLevelId,
  PaymentTermId=@PaymentTermId,
  CurrencyId=@CurrencyId,
  IncotermsId=@IncotermsId,
  PoDate=@PoDate,
  DeliveryDate=@DeliveryDate,
  Remarks=@Remarks,
  FxRate=@FxRate,
  Tax=@Tax,
  Shipping=@Shipping,
  Discount=@Discount,
  SubTotal=@SubTotal,
  NetTotal=@NetTotal,
  PoLines=@PoLines,
  UpdatedBy=@UpdatedBy,
  UpdatedDate=@UpdatedDate
WHERE Id=@Id AND IsActive=1;";
            await Connection.ExecuteAsync(sql, draft);
        }

        public async Task DeleteAsync(int id)
        {
            const string sql = @"UPDATE dbo.PurchaseOrderTemp SET IsActive=0, UpdatedDate=SYSUTCDATETIME() WHERE Id=@id;";
            await Connection.ExecuteAsync(sql, new { id });
        }

        public async Task<int> PromoteToPoAsync(int draftId, string promotedBy)
        {
            using var conn = _factory.CreateConnection();   // returns IDbConnection
            conn.Open();                                    // sync open is fine for Dapper

            using var tran = conn.BeginTransaction();

            try
            {
                var draft = await conn.QueryFirstOrDefaultAsync<PurchaseOrderTemp>(
                    "SELECT * FROM dbo.PurchaseOrderTemp WHERE Id=@id AND IsActive=1",
                    new { id = draftId }, tran);

                if (draft is null)
                    throw new InvalidOperationException("Draft not found or inactive.");

                // next PO number
                var lastId = await conn.ExecuteScalarAsync<int>(
                    "SELECT ISNULL(MAX(Id),0) FROM dbo.PurchaseOrder WITH (HOLDLOCK, TABLOCKX)",
                    transaction: tran);

                var poNo = $"PO-{lastId + 1:00000}";

                const string insertSql = @"
INSERT INTO dbo.PurchaseOrder
(PurchaseOrderNo, SupplierId, ApproveLevelId, ApprovalStatus, PaymentTermId, CurrencyId, IncotermsId,
 PoDate, DeliveryDate, Remarks, FxRate, Tax, Shipping, Discount, SubTotal, NetTotal, PoLines,
 CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
OUTPUT INSERTED.Id
VALUES
(@PurchaseOrderNo, @SupplierId, @ApproveLevelId, @ApprovalStatus, @PaymentTermId, @CurrencyId, @IncotermsId,
 @PoDate, @DeliveryDate, @Remarks, @FxRate, @Tax, @Shipping, @Discount, @SubTotal, @NetTotal, @PoLines,
 @CreatedBy, SYSUTCDATETIME(), @UpdatedBy, SYSUTCDATETIME(), 1);";

                var newId = await conn.ExecuteScalarAsync<int>(insertSql, new
                {
                    PurchaseOrderNo = poNo,
                    SupplierId = draft.SupplierId,
                    ApproveLevelId = draft.ApproveLevelId,
                    ApprovalStatus = draft.ApprovalStatus,
                    PaymentTermId = draft.PaymentTermId,
                    CurrencyId = draft.CurrencyId,
                    IncotermsId = draft.IncotermsId,
                    PoDate = draft.PoDate,
                    DeliveryDate = draft.DeliveryDate,
                    Remarks = draft.Remarks,
                    FxRate = draft.FxRate,
                    Tax = draft.Tax,
                    Shipping = draft.Shipping,
                    Discount = draft.Discount,
                    SubTotal = draft.SubTotal,
                    NetTotal = draft.NetTotal,
                    PoLines = draft.PoLines,
                    CreatedBy = draft.CreatedBy ?? promotedBy,
                    UpdatedBy = promotedBy
                }, tran);

                const string bumpPrSql = @"
;WITH PRs AS (
  SELECT DISTINCT prNo = LTRIM(RTRIM(prNo))
  FROM OPENJSON(@PoLines) WITH (prNo nvarchar(100) '$.prNo')
  WHERE ISNULL(prNo,'') <> ''
)
UPDATE PR
SET PR.Status = @ApprovalStatus,
    PR.UpdatedDate = SYSUTCDATETIME()
FROM PurchaseRequest PR
JOIN PRs ON PRs.prNo = PR.PurchaseRequestNo;";

                await conn.ExecuteAsync(bumpPrSql, new
                {
                    PoLines = draft.PoLines,
                    ApprovalStatus = 0
                }, tran);

                await conn.ExecuteAsync(
                    "UPDATE dbo.PurchaseOrderTemp SET IsActive=0, UpdatedBy=@promotedBy, UpdatedDate=SYSUTCDATETIME() WHERE Id=@id",
                    new { id = draftId, promotedBy }, tran);

                tran.Commit();
                return newId;
            }
            catch
            {
                tran.Rollback();
                throw;
            }
        }
    }
}
