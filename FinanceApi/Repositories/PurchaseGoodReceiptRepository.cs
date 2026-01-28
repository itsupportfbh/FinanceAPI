using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;

namespace FinanceApi.Repositories
{
    public class PurchaseGoodReceiptRepository : DynamicRepository, IPurchaseGoodReceiptRepository
    {
        private readonly IDbConnectionFactory _connectionFactory;

        public PurchaseGoodReceiptRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        public async Task<IEnumerable<PurchaseGoodReceiptItemsDTO>> GetAllAsync()
        {
            const string query = @"
SELECT
    pg.Id,
    pg.POID,
    pg.ReceptionDate,
    pg.OverReceiptTolerance,
    pg.GRNJson,
    pg.GrnNo,
    po.PoLines,
    po.CurrencyId,
    po.Tax,
    s.Id   AS SupplierId,
    s.Name AS SupplierName
FROM PurchaseGoodReceipt AS pg
JOIN PurchaseOrder AS po ON po.Id = pg.POID
LEFT JOIN Suppliers   AS s  ON s.Id = po.SupplierId
WHERE pg.IsActive = 1
  AND NOT EXISTS (
        SELECT 1
        FROM SupplierInvoicePin AS pin
        WHERE pin.GrnId   = pg.Id
          AND pin.IsActive = 1
          AND pin.GrnId IS NOT NULL
  );";

            return await Connection.QueryAsync<PurchaseGoodReceiptItemsDTO>(query);
        }

        public async Task<PurchaseGoodReceiptItemsDTO> GetByIdAsync(long id)
        {
            const string query = "SELECT * FROM PurchaseGoodReceipt WHERE Id = @Id";
            return await Connection.QuerySingleAsync<PurchaseGoodReceiptItemsDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(PurchaseGoodReceiptItems goodReceiptItemsDTO)
        {
            const string getLastGRNQuery = @"SELECT TOP 1 GrnNo FROM PurchaseGoodReceipt ORDER BY Id DESC";
            var lastGRN = await Connection.QueryFirstOrDefaultAsync<string>(getLastGRNQuery);
            int nextNumber = 1;

            if (!string.IsNullOrWhiteSpace(lastGRN) && lastGRN.StartsWith("GRN-"))
            {
                var numericPart = lastGRN.Substring(4);
                if (int.TryParse(numericPart, out int lastNumber))
                    nextNumber = lastNumber + 1;
            }

            var newGRN = $"GRN-{nextNumber:D4}";
            goodReceiptItemsDTO.GrnNo = newGRN;

            // ✅ 2) Copy SourceType/SourceRefId from PurchaseOrder using POID
            var src = await Connection.QueryFirstOrDefaultAsync<(string? SourceType, int? SourceRefId)>(@"
SELECT TOP 1
    SourceType,
    SourceRefId
FROM dbo.PurchaseOrder
WHERE Id = @POID;
", new { goodReceiptItemsDTO.POID });

            goodReceiptItemsDTO.SourceType = src.SourceType;
            goodReceiptItemsDTO.SourceRefId = src.SourceRefId;

            const string insertQuery = @"
INSERT INTO PurchaseGoodReceipt 
    (POID, ReceptionDate, OverReceiptTolerance, GRNJson, GrnNo, isActive,SourceType, SourceRefId) 
OUTPUT INSERTED.Id 
VALUES (@POID, @ReceptionDate, @OverReceiptTolerance, @GRNJson, @GrnNo, @isActive, @SourceType, @SourceRefId)";

            return await Connection.QueryFirstAsync<int>(insertQuery, goodReceiptItemsDTO);
        }

        public async Task<IEnumerable<PurchaseGoodReceiptItemsViewInfo>> GetAllDetailsAsync()
        {
            const string query = @"
SELECT 
  pg.Id AS ID,
  pg.ReceptionDate,
  po.PurchaseOrderNo AS PONO,
  pg.GrnNo,
  gd.itemCode,
  i.itemName AS ItemName,
  gd.supplierId,
  s.Name AS Name,
  gd.storageType,
  gd.surfaceTemp,
  gd.expiry,
  gd.pestSign,
  gd.drySpillage,
  gd.odor,
  gd.plateNumber,
  gd.defectLabels,
  gd.damagedPackage,
  gd.[time],
  gd.initial,
  gd.isFlagIssue,
  gd.isPostInventory,
  gd.qtyReceived,
  gd.qualityCheck,
  gd.batchSerial,
  gd.warehouseId,
  gd.binId,
  gd.strategyId,
  gd.warehouseName,
  gd.binName,
  gd.strategyName
FROM PurchaseGoodReceipt pg
OUTER APPLY OPENJSON(pg.GRNJson)
WITH (
    itemCode        NVARCHAR(50)  '$.itemCode',
    supplierId      INT           '$.supplierId',
    storageType     NVARCHAR(50)  '$.storageType',
    surfaceTemp     NVARCHAR(50)  '$.surfaceTemp',
    expiry          DATE          '$.expiry',
    pestSign        NVARCHAR(50)  '$.pestSign',
    drySpillage     NVARCHAR(50)  '$.drySpillage',
    odor            NVARCHAR(50)  '$.odor',
    plateNumber     NVARCHAR(50)  '$.plateNumber',
    defectLabels    NVARCHAR(100) '$.defectLabels',
    damagedPackage  NVARCHAR(50)  '$.damagedPackage',
    [time]          DATETIME2     '$.time',
    initial         NVARCHAR(MAX) '$.initial',
    isFlagIssue     BIT           '$.isFlagIssue',
    isPostInventory BIT           '$.isPostInventory',
    qtyReceived     INT           '$.qtyReceived',
    qualityCheck    NVARCHAR(MAX) '$.qualityCheck',
    batchSerial     NVARCHAR(MAX) '$.batchSerial',
	warehouseId     INT           '$.warehouseId',
    binId           INT           '$.binId',
	strategyId      INT           '$.strategyId',
    warehouseName   NVARCHAR(MAX) '$.warehouseName',
	binName         NVARCHAR(MAX) '$.binName',
    strategyName    NVARCHAR(MAX) '$.strategyName'
) AS gd
LEFT JOIN item i ON gd.itemCode = i.itemCode
LEFT JOIN Suppliers s ON gd.supplierId = s.Id
LEFT JOIN PurchaseOrder PO ON pg.POID = PO.Id
ORDER BY pg.Id DESC;";

            return await Connection.QueryAsync<PurchaseGoodReceiptItemsViewInfo>(query);
        }

        public async Task UpdateAsync(PurchaseGoodReceiptItems purchaseGoodReceipt)
        {
            const string query = "UPDATE PurchaseGoodReceipt SET GRNJSON = @GRNJSON WHERE Id = @ID";
            await Connection.ExecuteAsync(query, purchaseGoodReceipt);
        }

        public async Task UpdateGRN(PurchaseGoodReceiptItemsDTO purchaseGoodReceipt)
        {
            const string query = "UPDATE PurchaseGoodReceipt SET GRNJSON = @GRNJSON WHERE Id = @ID";
            await Connection.ExecuteAsync(query, purchaseGoodReceipt);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE PurchaseGoodReceipt SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }

        public async Task<IEnumerable<PurchaseGoodReceiptItemsDTO>> GetAllGRN()
        {
            const string query = @"
SELECT ID, POID, ReceptionDate, OverReceiptTolerance, GRNJson, FlagIssuesID, GrnNo
FROM PurchaseGoodReceipt
ORDER BY ID;";
            return await Connection.QueryAsync<PurchaseGoodReceiptItemsDTO>(query);
        }

        public async Task<IEnumerable<PurchaseGoodReceiptItemsDTO>> GetAllGRNByPoId()
        {
            const string query = @"
SELECT 
     pgr.ID,
     pgr.POID,
     pgr.ReceptionDate,
     pgr.OverReceiptTolerance,
     pgr.GRNJson,
     pgr.GrnNo,
     po.Tax
FROM PurchaseGoodReceipt as pgr 
INNER JOIN PurchaseOrder as po ON po.id= pgr.POID
ORDER BY pgr.ID;";
            return await Connection.QueryAsync<PurchaseGoodReceiptItemsDTO>(query);
        }

        // ===========================================================
        // ✅ GRN PostInventory -> update shortage SO lines + alloc + alert
        // ✅ EXACT signature as interface (NO extra param)
        // ===========================================================
        public async Task<ApplyGrnResult> ApplyGrnAndUpdateSalesOrderAsync(
     string itemCode, int? warehouseId, int? supplierId, int? binId, decimal receivedQty)
        {
            using var conn = _connectionFactory.CreateConnection();
            conn.Open();

            using var tx = conn.BeginTransaction();
            try
            {
                var itemId = await GetItemIdByCodeAsync(conn, tx, itemCode);
                if (itemId == null)
                    throw new Exception($"ItemCode not found in Item master: {itemCode}");

                var spResult = await conn.QuerySingleAsync<GrnAllocResult>(
                    "dbo.sp_GRN_LockSO_InsertAlloc_Debug",
                    new
                    {
                        ItemId = itemId.Value,
                        WarehouseId = warehouseId,
                        BinId = binId,
                        SupplierId = supplierId,
                        ReceivedQty = receivedQty,
                        CreatedBy = (int?)null
                    },
                    tx,
                    commandType: CommandType.StoredProcedure);

                // ✅ IMPORTANT: no throw. UpdatedLines==0 is allowed.
                tx.Commit();

                return new ApplyGrnResult
                {
                    ItemId = itemId.Value,
                    UpdatedSalesOrderLines = spResult.UpdatedLines,
                    Message = spResult.UpdatedLines > 0
                        ? "GRN applied and Sales Order updated."
                        : "GRN applied. No Sales Order lines to update."
                };
            }
            catch
            {
                try { tx.Rollback(); } catch { }
                throw;
            }
        }

        // 🔹 Get ItemId from ItemCode
        private async Task<int?> GetItemIdByCodeAsync(IDbConnection conn, IDbTransaction tx, string itemCode)
        {
            const string sql = @"
SELECT TOP 1 Id
FROM dbo.Item
WHERE ItemCode=@ItemCode AND IsActive=1;";
            return await conn.ExecuteScalarAsync<int?>(sql, new { ItemCode = itemCode }, tx);
        }

    }
}
