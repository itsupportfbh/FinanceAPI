using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Data;

namespace FinanceApi.Repositories
{
    public class PurchaseOrderRepository : DynamicRepository,IPurchaseOrderRepository
    {
        private readonly ApplicationDbContext _context;

        public PurchaseOrderRepository(IDbConnectionFactory connectionFactory)
        : base(connectionFactory)
        {

        }

        public async Task<IEnumerable<PurchaseOrderDto>> GetAllAsync()
        {
            //return await _context.PurchaseOrder
            //    .Where(c => c.IsActive)
            //    .OrderBy(c => c.Id)
            //    .Select(s => new PurchaseOrderDto
            //    {
            //        Id = s.Id,
            //        PurchaseOrderNo = s.PurchaseOrderNo,
            //        SupplierId = s.SupplierId,
            //        ApproveLevelId = s.ApproveLevelId,
            //        ApprovalStatus = s.ApprovalStatus,
            //        PaymentTermId = s.PaymentTermId,
            //        PaymentTermName = s.PaymentTerms.PaymentTermsName,
            //        CurrencyId = s.CurrencyId,
            //        CurrencyName = s.Currency.CurrencyName,
            //        DeliveryId = s.DeliveryId,
            //        DeliveryName = s.Location.Name,
            //        ContactNumber = s.ContactNumber,
            //        IncotermsId = s.IncotermsId,
            //        PoDate = s.PoDate,
            //        DeliveryDate = s.DeliveryDate,
            //        Remarks = s.Remarks,
            //        FxRate = s.FxRate,
            //        Tax = s.Tax,
            //        Shipping = s.Shipping,
            //        Discount = s.Discount,
            //        SubTotal = s.SubTotal,
            //        NetTotal = s.NetTotal,
            //        PoLines = s.PoLines,
            //        CreatedBy = s.CreatedBy,
            //        CreatedDate = s.CreatedDate,
            //        UpdatedBy = s.UpdatedBy,
            //        UpdatedDate = s.UpdatedDate,
            //        IsActive = s.IsActive
            //    })
            //    .ToListAsync();

            var query = @"
                        SELECT                            
                           po.Id ,
                           po.PurchaseOrderNo,
                           po.SupplierId,
                           ISNULL(s.Name, '') AS SupplierName,
                           po.ApproveLevelId,
                           po.ApprovalStatus,
                           po.PaymentTermId,
                           ISNULL(p.PaymentTermsName, '') AS PaymentTermName,
                           po.CurrencyId,
                           ISNULL(c.CurrencyName, '') AS CurrencyName,
                           po.IncotermsId,
                           po.PoDate,
                           po.DeliveryDate,
                           po.Remarks,
                           po.FxRate,
                           po.Tax,
                           po.Location,
                           po.ContactNumber,
                           po.Shipping,
                           po.Discount,
                           po.SubTotal,
                           po.NetTotal,
                           po.PoLines,  
                           po.CreatedBy,
                           po.CreatedDate,
                           po.UpdatedBy,
                           po.UpdatedDate,
                           po.IsActive,
                           s.Email

                           FROM PurchaseOrder po
                           LEFT JOIN Suppliers s ON po.SupplierId = s.Id
                           LEFT JOIN PaymentTerms p ON po.PaymentTermId = p.Id
                           LEFT JOIN Currency c ON po.CurrencyId = c.Id
                           WHERE po.IsActive = 1
                           ORDER BY po.Id";

            var rows = await Connection.QueryAsync<PurchaseOrderDto>(query);
            return rows.ToList();
        }




        public async Task<IEnumerable<PurchaseOrderDto>> GetAllDetailswithGRN()
        {
           
            var query = @"
                        SELECT
    po.Id,
    po.PurchaseOrderNo,
    po.SupplierId,
    ISNULL(s.Name, '') AS SupplierName,
    po.ApproveLevelId,
    po.ApprovalStatus,
    po.PaymentTermId,
    ISNULL(p.PaymentTermsName, '') AS PaymentTermName,
    po.CurrencyId,
    ISNULL(c.CurrencyName, '') AS CurrencyName,
    po.IncotermsId,
    po.PoDate,
    po.DeliveryDate,
    po.Remarks,
    po.FxRate,
    po.Tax,
    po.Location,
    po.ContactNumber,
    po.Shipping,
    po.Discount,
    po.SubTotal,
    po.NetTotal,
    po.PoLines,
    po.CreatedBy,
    po.CreatedDate,
    po.UpdatedBy,
    po.UpdatedDate,
    po.IsActive
FROM PurchaseOrder po
LEFT JOIN Suppliers s ON po.SupplierId = s.Id
LEFT JOIN PaymentTerms p ON po.PaymentTermId = p.Id
LEFT JOIN Currency c ON po.CurrencyId = c.Id
WHERE po.IsActive = 1 
  AND po.ApprovalStatus = 2
  AND po.Id NOT IN (
      SELECT POID FROM PurchaseGoodReceipt WHERE POID IS NOT NULL
  )
ORDER BY po.Id;
";

            var rows = await Connection.QueryAsync<PurchaseOrderDto>(query);
            return rows.ToList();
        }


        public async Task<PurchaseOrderDto?> GetByIdAsync(int id)
        {
            const string sql = @"
                           SELECT
                           po.Id ,
                           po.PurchaseOrderNo,
                           po.SupplierId,
                           ISNULL(s.Name, '') AS SupplierName,
                           po.ApproveLevelId,
                           po.ApprovalStatus,
                           po.PaymentTermId,
                           ISNULL(p.PaymentTermsName, '') AS PaymentTermName,
                           po.CurrencyId,
                           ISNULL(c.CurrencyName, '') AS CurrencyName,
                           po.IncotermsId,
                           po.PoDate,
                           po.DeliveryDate,
                           po.Remarks,
                           po.FxRate,
                           po.Tax,
                           po.Location,
                           po.ContactNumber,
                           po.Shipping,
                           po.Discount,
                           po.SubTotal,
                           po.NetTotal,
                           po.PoLines,  
                           po.CreatedBy,
                           po.CreatedDate,
                           po.UpdatedBy,
                           po.UpdatedDate,
                           po.IsActive,
                           s.Email

                           FROM PurchaseOrder po
                           LEFT JOIN Suppliers s ON po.SupplierId = s.Id
                           LEFT JOIN PaymentTerms p ON po.PaymentTermId = p.Id
                           LEFT JOIN Currency c ON po.CurrencyId = c.Id
                           WHERE po.Id = @id AND po.IsActive = 1;";

            var result = await Connection.QueryFirstOrDefaultAsync<PurchaseOrderDto>(sql, new { id });
            return result;
        }


        public async Task<int> CreateAsync(PurchaseOrder purchaseOrder)
        {
            if (purchaseOrder is null) throw new ArgumentNullException(nameof(purchaseOrder));

            // Timestamps
            var now = DateTime.UtcNow;
            if (purchaseOrder.CreatedDate == default) purchaseOrder.CreatedDate = now;
            if (purchaseOrder.UpdatedDate == default) purchaseOrder.UpdatedDate = now;

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {
                // 1) Generate safe running number
                // (HOLDLOCK + TABLOCKX to serialize access)
                const string getLastIdSql = @"SELECT ISNULL(MAX(Id), 0) FROM dbo.PurchaseOrder WITH (HOLDLOCK, TABLOCKX);";
                var lastId = await conn.ExecuteScalarAsync<int>(getLastIdSql, transaction: tx);
                var nextNumber = lastId + 1;
                purchaseOrder.PurchaseOrderNo = $"PO-{nextNumber:00000}";

                // ✅ 1.5) Derive SourceType/SourceRefId from PRs in PoLines
                var src = await conn.QueryFirstOrDefaultAsync<(string? SourceType, int? SourceRefId)>(@"
;WITH PRs AS (
    SELECT DISTINCT prNo = LTRIM(RTRIM(prNo))
    FROM OPENJSON(@PoLines)
    WITH (
        __fromPR bit          '$.__fromPR',
        prNo     nvarchar(50) '$.prNo'
    )
    WHERE ISNULL(__fromPR, 0) = 1
      AND ISNULL(prNo, '') <> ''
),
Src AS (
    SELECT
        COUNT(*) AS Cnt,
        COUNT(DISTINCT ISNULL(PR.SourceType,'')) AS SrcTypeCnt,
        COUNT(DISTINCT ISNULL(CONVERT(nvarchar(50),PR.SourceRefId),'')) AS SrcRefCnt,
        MIN(PR.SourceType) AS SourceType,
        MIN(PR.SourceRefId) AS SourceRefId
    FROM dbo.PurchaseRequest PR
    JOIN PRs ON PR.PurchaseRequestNo = PRs.prNo
)
SELECT
    SourceType =
        CASE WHEN Cnt = 0 THEN NULL
             WHEN SrcTypeCnt = 1 THEN SourceType
             ELSE 'MIXED' END,
    SourceRefId =
        CASE WHEN Cnt = 0 THEN NULL
             WHEN SrcRefCnt = 1 THEN SourceRefId
             ELSE NULL END
FROM Src;
", new { PoLines = purchaseOrder.PoLines }, tx);

                // set into PO object
                purchaseOrder.SourceType = src.SourceType;
                purchaseOrder.SourceRefId = src.SourceRefId;


                // 2) Insert PO and capture Id
                const string insertPoSql = @"
INSERT INTO dbo.PurchaseOrder
(
    PurchaseOrderNo, SupplierId, ApproveLevelId, ApprovalStatus, PaymentTermId,
    CurrencyId, IncotermsId, PoDate, DeliveryDate, Remarks, FxRate, Tax,Location,ContactNumber,Shipping,
    Discount, SubTotal, NetTotal, PoLines, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive,SourceType, SourceRefId
)
OUTPUT INSERTED.Id
VALUES
(
    @PurchaseOrderNo, @SupplierId, @ApproveLevelId, @ApprovalStatus, @PaymentTermId,
    @CurrencyId, @IncotermsId, @PoDate, @DeliveryDate, @Remarks, @FxRate, @Tax,@Location,@ContactNumber, @Shipping,
    @Discount, @SubTotal, @NetTotal, @PoLines, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive,@SourceType, @SourceRefId
);";

                var poId = await conn.ExecuteScalarAsync<int>(insertPoSql, purchaseOrder, tx);

                // 3) Update PurchaseRequest status from prNo inside @PoLines JSON
                const string updatePrSql = @"
;WITH PRs AS (
    SELECT DISTINCT prNo = LTRIM(RTRIM(prNo))
    FROM OPENJSON(@PoLines)
    WITH (prNo nvarchar(100) '$.prNo')
    WHERE ISNULL(prNo, '') <> ''
)
UPDATE PR
SET PR.Status      = @ApprovalStatus,
    PR.UpdatedDate = SYSUTCDATETIME()
FROM dbo.PurchaseRequest PR
JOIN PRs ON PRs.prNo = PR.PurchaseRequestNo;";

                await conn.ExecuteAsync(
                    updatePrSql,
                    new
                    {
                        PoLines = purchaseOrder.PoLines,          // JSON string (must contain prNo per line)
                        ApprovalStatus = purchaseOrder.ApprovalStatus
                    },
                    tx);

                // 4) Collect StockReorderId(s) from updated PRs and set Status = 2 (or = @ApprovalStatus if preferred)
                const string updateStockReorderSql = @"
;WITH PRs AS (
    SELECT DISTINCT prNo = LTRIM(RTRIM(prNo))
    FROM OPENJSON(@PoLines)
    WITH (prNo nvarchar(100) '$.prNo')
    WHERE ISNULL(prNo, '') <> ''
),
SRids AS (
    SELECT DISTINCT PR.StockReorderId
    FROM dbo.PurchaseRequest PR
    JOIN PRs ON PRs.prNo = PR.PurchaseRequestNo
    WHERE PR.StockReorderId IS NOT NULL
)
UPDATE SR
SET SR.Status      = @ApprovalStatus,                    -- <-- change to @ApprovalStatus to mirror PO status
    SR.UpdatedDate = SYSUTCDATETIME()
FROM dbo.StockReorder SR
JOIN SRids X ON X.StockReorderId = SR.Id;";

                await conn.ExecuteAsync(
                    updateStockReorderSql,
                    new { PoLines = purchaseOrder.PoLines, purchaseOrder.ApprovalStatus },
                    tx);

                // 5) Set Status = 2 for child StockReorderLines
                const string updateStockReorderLinesSql = @"
;WITH PRs AS (
    SELECT DISTINCT prNo = LTRIM(RTRIM(prNo))
    FROM OPENJSON(@PoLines)
    WITH (prNo nvarchar(100) '$.prNo')
    WHERE ISNULL(prNo, '') <> ''
),
SRids AS (
    SELECT DISTINCT PR.StockReorderId
    FROM dbo.PurchaseRequest PR
    JOIN PRs ON PRs.prNo = PR.PurchaseRequestNo
    WHERE PR.StockReorderId IS NOT NULL
)
UPDATE L
SET L.Status      = @ApprovalStatus,                    
    L.UpdatedDate = SYSUTCDATETIME()
FROM dbo.StockReorderLines L
JOIN SRids X ON X.StockReorderId = L.StockReorderId;
-- If you only want to flip selected lines, add: WHERE L.Selected = 1
";

                await conn.ExecuteAsync(
                    updateStockReorderLinesSql,
                    new { PoLines = purchaseOrder.PoLines, purchaseOrder.ApprovalStatus },
                    tx);

                tx.Commit();
                return poId;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }


        public async Task UpdateAsync(PurchaseOrder updatedPurchaseOrder)
        {
            if (updatedPurchaseOrder is null) throw new ArgumentNullException(nameof(updatedPurchaseOrder));

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {

                // ✅ 1.5) Derive SourceType/SourceRefId from PRs in PoLines
                var src = await conn.QueryFirstOrDefaultAsync<(string? SourceType, int? SourceRefId)>(@"
;WITH PRs AS (
    SELECT DISTINCT prNo = LTRIM(RTRIM(prNo))
    FROM OPENJSON(@PoLines)
    WITH (
        __fromPR bit          '$.__fromPR',
        prNo     nvarchar(50) '$.prNo'
    )
    WHERE ISNULL(__fromPR, 0) = 1
      AND ISNULL(prNo, '') <> ''
),
Src AS (
    SELECT
        COUNT(*) AS Cnt,
        COUNT(DISTINCT ISNULL(PR.SourceType,'')) AS SrcTypeCnt,
        COUNT(DISTINCT ISNULL(CONVERT(nvarchar(50),PR.SourceRefId),'')) AS SrcRefCnt,
        MIN(PR.SourceType) AS SourceType,
        MIN(PR.SourceRefId) AS SourceRefId
    FROM dbo.PurchaseRequest PR
    JOIN PRs ON PR.PurchaseRequestNo = PRs.prNo
)
SELECT
    SourceType =
        CASE WHEN Cnt = 0 THEN NULL
             WHEN SrcTypeCnt = 1 THEN SourceType
             ELSE 'MIXED' END,
    SourceRefId =
        CASE WHEN Cnt = 0 THEN NULL
             WHEN SrcRefCnt = 1 THEN SourceRefId
             ELSE NULL END
FROM Src;
", new { PoLines = updatedPurchaseOrder.PoLines }, tx);

                updatedPurchaseOrder.SourceType = src.SourceType;
                updatedPurchaseOrder.SourceRefId = src.SourceRefId;


                // 1) Update PO header
                const string updatePoSql = @"
UPDATE dbo.PurchaseOrder 
SET 
    PurchaseOrderNo = @PurchaseOrderNo,
    SupplierId      = @SupplierId,
    ApproveLevelId  = @ApproveLevelId,
    ApprovalStatus  = @ApprovalStatus,
    PaymentTermId   = @PaymentTermId,
    CurrencyId      = @CurrencyId,
    IncotermsId     = @IncotermsId,
    PoDate          = @PoDate,
    DeliveryDate    = @DeliveryDate,
    Remarks         = @Remarks,
    FxRate          = @FxRate,
    Tax             = @Tax,
    Location        = @Location,
    ContactNumber   = @ContactNumber,
    Shipping        = @Shipping,
    Discount        = @Discount,
    SubTotal        = @SubTotal,
    NetTotal        = @NetTotal,
    PoLines         = @PoLines,
    SourceType     = @SourceType,
    SourceRefId    = @SourceRefId,
    UpdatedBy       = @UpdatedBy,
    UpdatedDate     = @UpdatedDate,
    IsActive        = @IsActive
WHERE Id = @Id;
";
                await conn.ExecuteAsync(updatePoSql, updatedPurchaseOrder, tx);

                // 2) Update PR statuses for PRs referenced in PoLines (expects each line to carry `prNo`)
                const string updatePrSql = @"
;WITH PRs AS (
    SELECT DISTINCT prNo = LTRIM(RTRIM(prNo))
    FROM OPENJSON(@PoLines)
    WITH (prNo nvarchar(100) '$.prNo')
    WHERE ISNULL(prNo, '') <> ''
)
UPDATE PR
SET PR.Status      = @ApprovalStatus,
    PR.UpdatedDate = SYSUTCDATETIME()
FROM dbo.PurchaseRequest PR
JOIN PRs ON PRs.prNo = PR.PurchaseRequestNo;
";
                await conn.ExecuteAsync(
                    updatePrSql,
                    new { PoLines = updatedPurchaseOrder.PoLines, updatedPurchaseOrder.ApprovalStatus },
                    tx);

                // 3) Update StockReorder status (derived via linked PRs) → set to 2 (or mirror PO status)
                const string updateStockReorderSql = @"
;WITH PRs AS (
    SELECT DISTINCT prNo = LTRIM(RTRIM(prNo))
    FROM OPENJSON(@PoLines)
    WITH (prNo nvarchar(100) '$.prNo')
    WHERE ISNULL(prNo, '') <> ''
),
SRids AS (
    SELECT DISTINCT PR.StockReorderId
    FROM dbo.PurchaseRequest PR
    JOIN PRs ON PRs.prNo = PR.PurchaseRequestNo
    WHERE PR.StockReorderId IS NOT NULL
)
UPDATE SR
SET SR.Status      = @ApprovalStatus,                    -- <== change to @ApprovalStatus to mirror PO status
    SR.UpdatedDate = SYSUTCDATETIME()
FROM dbo.StockReorder SR
JOIN SRids X ON X.StockReorderId = SR.Id;
";
                await conn.ExecuteAsync(
                    updateStockReorderSql,
                    new { PoLines = updatedPurchaseOrder.PoLines, updatedPurchaseOrder.ApprovalStatus },
                    tx);

                // 4) Update StockReorderLines status (same SR set)
                const string updateStockReorderLinesSql = @"
;WITH PRs AS (
    SELECT DISTINCT prNo = LTRIM(RTRIM(prNo))
    FROM OPENJSON(@PoLines)
    WITH (prNo nvarchar(100) '$.prNo')
    WHERE ISNULL(prNo, '') <> ''
),
SRids AS (
    SELECT DISTINCT PR.StockReorderId
    FROM dbo.PurchaseRequest PR
    JOIN PRs ON PRs.prNo = PR.PurchaseRequestNo
    WHERE PR.StockReorderId IS NOT NULL
)
UPDATE L
SET L.Status      = @ApprovalStatus,                     -- <== change to @ApprovalStatus to mirror PO status
    L.UpdatedDate = SYSUTCDATETIME()
FROM dbo.StockReorderLines L
JOIN SRids X ON X.StockReorderId = L.StockReorderId;
-- If you only want to update selected lines, add: WHERE L.Selected = 1
";
                await conn.ExecuteAsync(
                    updateStockReorderLinesSql,
                    new { PoLines = updatedPurchaseOrder.PoLines, updatedPurchaseOrder.ApprovalStatus },
                    tx);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }




        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE PurchaseOrder SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }

        public async Task<(string Email, string SupplierName, string PoNo, int ApprovalStatus)>
    GetSupplierEmailMetaAsync(int poId)
        {
            const string sql = @"
SELECT 
    ISNULL(s.Email,'') AS Email,
    ISNULL(s.Name,'')  AS SupplierName,
    ISNULL(po.PurchaseOrderNo,'') AS PoNo,
    ISNULL(po.ApprovalStatus,0) AS ApprovalStatus
FROM PurchaseOrder po
LEFT JOIN Suppliers s ON s.Id = po.SupplierId
WHERE po.Id = @poId AND po.IsActive = 1;
";

            var row = await Connection.QueryFirstOrDefaultAsync(sql, new { poId });

            if (row == null)
                return ("", "", "", 0);

            return (
                (string)row.Email,
                (string)row.SupplierName,
                (string)row.PoNo,
                (int)row.ApprovalStatus
            );
        }

    }
}
