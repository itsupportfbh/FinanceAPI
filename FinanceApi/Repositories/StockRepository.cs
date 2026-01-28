using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Connections;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Data.Common;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class StockRepository : DynamicRepository,IStockRepository
    {

        public StockRepository(IDbConnectionFactory connectionFactory)
     : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<StockDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Stock";

            return await Connection.QueryAsync<StockDTO>(query);
        }


        public async Task<StockDTO> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM Stock WHERE Id = @Id";

            return await Connection.QuerySingleAsync<StockDTO>(query, new { Id = id });
        }

        public async Task<int> InsertBulkAsync(IEnumerable<Stock> stocks)
        {
            const string query = @"
INSERT INTO [dbo].[Stock] (
    ItemID,
    MrId,                -- ✅ NEW
    FromWarehouseID,
    ToWarehouseID,
    Available,
    OnHand,
    isApproved,
    CreatedBy,
    CreatedDate,
    UpdatedBy,
    UpdatedDate,
    FromWarehouseName,
    ItemName,
    Sku,
    BinId,
    BinName,
    Remarks,
    SupplierId,       
    IsSupplierBased,
    ToBinId,
Status
)
VALUES (
    @ItemId,
    @MrId,               -- ✅ NEW
    @FromWarehouseID,
    @ToWarehouseID,
    @Available,
    @OnHand,
    @isApproved,
    @CreatedBy,
    @CreatedDate,
    @UpdatedBy,
    @UpdatedDate,
    @FromWarehouseName,
    @ItemName,
    @Sku,
    @BinId,
    @BinName,
    @Remarks,
    @SupplierId,       
    @IsSupplierBased,
    @ToBinId,
1
);";

            return await Connection.ExecuteAsync(query, stocks);
        }






        public async Task UpdateAsync(Stock stock)
        {
            const string query = "UPDATE Stock SET OnHand = @OnHand,Available =@Available WHERE Id = @Id";
            await Connection.ExecuteAsync(query, stock);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Stock SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }


        public async Task<IEnumerable<StockListViewInfo>> GetAllStockList()
        {
            const string query = @"
WITH StockSummary AS (
    SELECT
        iws.ItemId,
        iws.WarehouseId,
        SUM(iws.OnHand) AS OnHand,
        SUM(iws.Available) AS Available,
        SUM(iws.Reserved) AS Reserved,
        MAX(iws.MinQty) AS MinQty,
        MAX(iws.MaxQty) AS MaxQty,
        MAX(bn.Id) AS BinId,               -- ✅ fixed here
        MAX(bn.BinName) AS BinName
    FROM ItemWarehouseStock iws
    INNER JOIN BIN bn ON bn.Id = iws.BinId -- ✅ fixed join column
    GROUP BY iws.ItemId, iws.WarehouseId
),
SupplierList AS (
    SELECT
        ip.ItemId,
        ip.WarehouseId,
        ip.SupplierId,
        s.Name AS SupplierName,
        ip.Qty,
        ROW_NUMBER() OVER(PARTITION BY ip.ItemId, ip.WarehouseId ORDER BY ip.SupplierId) AS rn
    FROM ItemPrice ip
    INNER JOIN Suppliers s ON s.Id = ip.SupplierId
)
SELECT
    im.Id,
    im.Name,
    im.Sku,
    wh.Id AS WarehouseId,
    wh.Name AS WarehouseName,
    ss.BinId,
    ss.BinName,
    sl.SupplierId,
    sl.SupplierName,
    ss.OnHand,
    CASE WHEN sl.rn = 1 THEN ss.Reserved ELSE 0 END AS Reserved,  -- ✅ Reserved only once per warehouse
    ss.Available,
    ss.MinQty,
    ss.MaxQty,
    im.ExpiryDate,
    im.Category,
    im.Uom,
    sl.Qty AS Qty
FROM SupplierList sl
INNER JOIN StockSummary ss 
    ON ss.ItemId = sl.ItemId 
    AND ss.WarehouseId = sl.WarehouseId
INNER JOIN ItemMaster im ON im.Id = sl.ItemId
INNER JOIN Warehouse wh ON wh.Id = sl.WarehouseId
ORDER BY im.Name, wh.Name, sl.SupplierName;

";

            return await Connection.QueryAsync<StockListViewInfo>(query);
        }


        public async Task<IEnumerable<StockListViewInfo>> GetAllItemStockList()
        {
            const string query = @"
              select im.Id,im.Name,im.Sku,
iws.WarehouseId,wh.Name as WarehouseName,
iws.BinId,bn.BinName,
iws.Available,
iws.OnHand,iws.MinQty,iws.MaxQty,iws.Reserved,im.ExpiryDate,im.Category,im.Uom
from ItemMaster as im
inner join ItemWarehouseStock as iws on iws.ItemId = im.Id
inner join Warehouse as wh on wh.Id = iws.WarehouseId
inner join BIN as bn on bn.ID = iws.BinId;
";

            return await Connection.QueryAsync<StockListViewInfo>(query);
        }




        public async Task<int> MarkAsTransferredBulkAsync(IEnumerable<MarkAsTransferredRequest> requests)
        {
            const string updateIws = @"
UPDATE iws
SET    IsTransfered = 1
FROM   [dbo].[ItemWarehouseStock] iws
WHERE  iws.ItemId      = @ItemId
  AND  iws.WarehouseId = @WarehouseId
  AND (
        (iws.BinId IS NULL AND @BinId IS NULL)
        OR iws.BinId = @BinId
      );";

            const string updateItemPrice = @"
UPDATE ip
SET    IsTransfered = 1
FROM   [dbo].[ItemPrice] ip
WHERE  ip.ItemId      = @ItemId
  AND  ip.WarehouseId = @WarehouseId
  AND (
        (ip.SupplierId IS NULL AND @SupplierId IS NULL)
        OR ip.SupplierId = @SupplierId
      );";

            // ✅ Cast to SqlConnection for async support
            var sqlConn = (SqlConnection)Connection;

            if (sqlConn.State != ConnectionState.Open)
                await sqlConn.OpenAsync();

            using var tx = sqlConn.BeginTransaction();
            try
            {
                // Run both updates in a single transaction
                var a = await sqlConn.ExecuteAsync(updateIws, requests, tx);
                var b = await sqlConn.ExecuteAsync(updateItemPrice, requests, tx);

                tx.Commit();
                return a + b; // total rows affected
            }
            catch
            {
                tx.Rollback();
                throw;
            }
            finally
            {
                await sqlConn.CloseAsync();
            }
        }





//        public async Task<IEnumerable<StockTransferListViewInfo>> GetAllStockTransferedList()
//        {
//            const string query = @"
//WITH RS AS (
//  SELECT
//      iws.ItemId,
//      iws.WarehouseId,
//      SUM(ISNULL(iws.Reserved,0)) AS Reserved,
//      MAX(ISNULL(iws.MinQty,0))   AS MinQty,
//      MAX(ISNULL(iws.MaxQty,0))   AS MaxQty
//  FROM ItemWarehouseStock iws
//  GROUP BY iws.ItemId, iws.WarehouseId
//)
//SELECT
//    s.Id                  AS StockId,
//    im.Id                 AS ItemId,
//    im.Name,
//    im.Sku,

//    s.FromWarehouseID     AS WarehouseId,
//    whFrom.Name           AS WarehouseName,

//    s.FromWarehouseID,
//    whFrom.Name           AS FromWarehouseName,
//    s.ToWarehouseID,
//    whTo.Name             AS ToWarehouseName,

//    s.BinId,
//    bn.BinName,

//    ISNULL(iws.OnHand,0)      AS OnHand,
//    ISNULL(iws.Available,0)   AS Available,
//    ISNULL(rs.Reserved,0)     AS Reserved,
//    ISNULL(rs.MinQty,0)       AS MinQty,
//    ISNULL(rs.MaxQty,0)       AS MaxQty,

//    lp.SupplierId,
//    lp.Price,
//    sp.Name               AS SupplierName,

//    s.TransferQty,
//    s.Remarks,
//    s.IsApproved,
//    s.IsSupplierBased
//FROM Stock s
//JOIN ItemMaster im ON im.Id = s.ItemId

//OUTER APPLY (
//    SELECT TOP (1) ip.SupplierId, ip.Price
//    FROM ItemPrice ip
//    WHERE ip.ItemId = s.ItemId
//      AND (ip.SupplierId = s.SupplierId OR s.SupplierId IS NULL)
//      AND ip.WarehouseId = s.FromWarehouseID
//    ORDER BY ip.Id DESC
//) lp

//LEFT JOIN Suppliers sp   ON sp.Id     = lp.SupplierId
//LEFT JOIN Warehouse whFrom ON whFrom.Id = s.FromWarehouseID
//LEFT JOIN Warehouse whTo   ON whTo.Id   = s.ToWarehouseID
//LEFT JOIN Bin bn           ON bn.Id     = s.BinId

//LEFT JOIN ItemWarehouseStock iws
//       ON iws.ItemId = s.ItemId
//      AND iws.WarehouseId = s.FromWarehouseID
//      AND (iws.BinId = s.BinId OR s.BinId IS NULL)

//LEFT JOIN RS rs
//       ON rs.ItemId      = s.ItemId
//      AND rs.WarehouseId = s.FromWarehouseID

//WHERE s.isApproved = 1    
//ORDER BY s.FromWarehouseID, lp.SupplierId;

//";
//            return await Connection.QueryAsync<StockTransferListViewInfo>(query);
//        }


        public async Task<IEnumerable<StockTransferListViewInfo>> GetStockTransferedList()
        {
            const string query = @"
WITH RS AS (
  SELECT
      iws.ItemId,
      iws.WarehouseId,
      SUM(ISNULL(iws.Reserved,0)) AS Reserved,
      MAX(ISNULL(iws.MinQty,0))   AS MinQty,
      MAX(ISNULL(iws.MaxQty,0))   AS MaxQty
  FROM ItemWarehouseStock iws
  GROUP BY iws.ItemId, iws.WarehouseId
)
SELECT
    s.Id                  AS StockId,
    im.Id                 AS ItemId,
    im.Name,
    im.Sku,

    s.FromWarehouseID     AS WarehouseId,
    whFrom.Name           AS WarehouseName,

    s.FromWarehouseID,
    whFrom.Name           AS FromWarehouseName,
    s.ToWarehouseID,
    whTo.Name             AS ToWarehouseName,

    s.BinId,
    bn.BinName,

    ISNULL(iws.OnHand,0)      AS OnHand,
    ISNULL(iws.Available,0)   AS Available,
    ISNULL(rs.Reserved,0)     AS Reserved,
    ISNULL(rs.MinQty,0)       AS MinQty,
    ISNULL(rs.MaxQty,0)       AS MaxQty,

    lp.SupplierId,
    lp.Price,
    sp.Name               AS SupplierName,

    s.TransferQty,
    s.Remarks,
    s.IsApproved,
    s.IsSupplierBased
FROM Stock s
JOIN ItemMaster im ON im.Id = s.ItemId

OUTER APPLY (
    SELECT TOP (1) ip.SupplierId, ip.Price
    FROM ItemPrice ip
    WHERE ip.ItemId = s.ItemId
      AND (ip.SupplierId = s.SupplierId OR s.SupplierId IS NULL)
      AND ip.WarehouseId = s.FromWarehouseID
    ORDER BY ip.Id DESC
) lp

LEFT JOIN Suppliers sp   ON sp.Id     = lp.SupplierId
LEFT JOIN Warehouse whFrom ON whFrom.Id = s.FromWarehouseID
LEFT JOIN Warehouse whTo   ON whTo.Id   = s.ToWarehouseID
LEFT JOIN Bin bn           ON bn.Id     = s.BinId

LEFT JOIN ItemWarehouseStock iws
       ON iws.ItemId = s.ItemId
      AND iws.WarehouseId = s.FromWarehouseID
      AND (iws.BinId = s.BinId OR s.BinId IS NULL)

LEFT JOIN RS rs
       ON rs.ItemId      = s.ItemId
      AND rs.WarehouseId = s.FromWarehouseID

WHERE iws.IsTransfered =1
ORDER BY s.FromWarehouseID, lp.SupplierId;

";
            return await Connection.QueryAsync<StockTransferListViewInfo>(query);
        }




        public async Task<int> AdjustOnHandAsync(AdjustOnHandRequest request)
        {
            const string updateIwsSql = @"
UPDATE iws
SET 
    iws.OnHand = iws.OnHand - @FaultQty,   -- ✅ subtract fault qty
    iws.Available = CASE 
                        WHEN (iws.OnHand - @FaultQty) - ISNULL(iws.Reserved, 0) < 0 
                        THEN 0 
                        ELSE (iws.OnHand - @FaultQty) - ISNULL(iws.Reserved, 0) 
                    END,
    iws.StockIssueID = @StockIssueId,
    iws.ApprovedBy = @ApprovedBy
FROM [dbo].[ItemWarehouseStock] iws
WHERE iws.ItemId = @ItemId
  AND iws.WarehouseId = @WarehouseId
  AND ((iws.BinId IS NULL AND @BinId IS NULL) OR iws.BinId = @BinId);
";

            const string updateItemPriceSql = @"
UPDATE ip
SET ip.Qty = @FinalOnHand     -- ✅ update full new on-hand
FROM [dbo].[ItemPrice] ip
WHERE ip.ItemId = @ItemId
  AND ip.WarehouseId = @WarehouseId
  AND ip.SupplierId = @SupplierId;
";

            var conn = (SqlConnection)Connection;

            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {
                // 1️⃣ Update ItemWarehouseStock (subtract fault quantity)
                var rowsIws = await conn.ExecuteAsync(updateIwsSql, new
                {
                    request.ItemId,
                    request.WarehouseId,
                    request.BinId,
                    request.FaultQty,
                    request.StockIssueId,
                    request.ApprovedBy
                }, tx);

                // 2️⃣ Update ItemPrice (set full final on-hand)
                var rowsIp = await conn.ExecuteAsync(updateItemPriceSql, new
                {
                    request.ItemId,
                    request.WarehouseId,
                    request.SupplierId,
                    request.FinalOnHand
                }, tx);

                tx.Commit();
                return rowsIws + rowsIp;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }




        public async Task ApproveTransfersBulkAsync(IEnumerable<TransferApproveRequest> transfers)
        {
            if (transfers == null) throw new ArgumentNullException(nameof(transfers));

            using var conn = Connection;
            if (conn.State != ConnectionState.Open) conn.Open();

            using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);

            try
            {
                // ✅ Track MR headers we touched
                var touchedMrIds = new HashSet<int>();

                foreach (var t in transfers)
                {
                    if (t.TransferQty <= 0)
                        throw new Exception($"TransferQty must be > 0. StockId={t.StockId}, ItemId={t.ItemId}");

                    // -------------------------
                    // 1) Read current balances WITH LOCK
                    // -------------------------

                    // ItemPrice (source)
                    var ip = await conn.QueryFirstOrDefaultAsync<(int Id, decimal Qty)>(@"
SELECT TOP 1 Id, CAST(Qty as decimal(18,4)) as Qty
FROM dbo.ItemPrice WITH (UPDLOCK, ROWLOCK)
WHERE ItemId=@ItemId
  AND (@SupplierId IS NULL OR SupplierId=@SupplierId)
  AND WarehouseId=@WarehouseId
ORDER BY Id DESC;
", new { t.ItemId, t.SupplierId, WarehouseId = t.WarehouseId }, tx);

                    if (ip.Id == 0)
                        throw new Exception($"ItemPrice not found for ItemId={t.ItemId}, SupplierId={t.SupplierId}, WarehouseId={t.WarehouseId}");

                    if (ip.Qty < t.TransferQty)
                        throw new Exception($"ItemPrice Qty not enough. Available={ip.Qty}, Transfer={t.TransferQty} (ItemId={t.ItemId})");

                    // ItemWarehouseStock (source) - binless
                    var ws = await conn.QueryFirstOrDefaultAsync<(int Id, decimal OnHand, decimal Available)>(@"
SELECT TOP 1 Id,
       CAST(OnHand as decimal(18,4)) as OnHand,
       CAST(Available as decimal(18,4)) as Available
FROM dbo.ItemWarehouseStock WITH (UPDLOCK, ROWLOCK)
WHERE ItemId=@ItemId
  AND WarehouseId=@WarehouseId
ORDER BY Id DESC;
", new { t.ItemId, WarehouseId = t.WarehouseId }, tx);

                    if (ws.Id == 0)
                        throw new Exception($"ItemWarehouseStock not found for ItemId={t.ItemId}, WarehouseId={t.WarehouseId}");

                    if (ws.Available < t.TransferQty)
                        throw new Exception($"ItemWarehouseStock Available not enough. Available={ws.Available}, Transfer={t.TransferQty} (ItemId={t.ItemId})");

                    // Stock row
                    var st = await conn.QueryFirstOrDefaultAsync<(int Id, decimal OnHand, decimal Available, decimal TransferQty, int MrId)>(@"
SELECT TOP 1 ID as Id,
       CAST(OnHand as decimal(18,4)) as OnHand,
       CAST(Available as decimal(18,4)) as Available,
       CAST(ISNULL(TransferQty,0) as decimal(18,4)) as TransferQty,
       CAST(ISNULL(MrId,0) as int) as MrId
FROM dbo.Stock WITH (UPDLOCK, ROWLOCK)
WHERE ID=@StockId AND ItemId=@ItemId;
", new { t.StockId, t.ItemId }, tx);

                    if (st.Id == 0)
                        throw new Exception($"Stock row not found for StockId={t.StockId}, ItemId={t.ItemId}");

                    if (st.Available < t.TransferQty)
                        throw new Exception($"Stock.Available not enough. Available={st.Available}, Transfer={t.TransferQty} (StockId={t.StockId})");

                    // -------------------------
                    // 2) Decide Partial / Full (per this line request)
                    // -------------------------
                    bool isFull = (t.RequestedQty > 0 && t.TransferQty >= t.RequestedQty);
                    bool isPartial = (t.RequestedQty > 0 && t.TransferQty < t.RequestedQty);

                    // -------------------------
                    // 3) Update ItemPrice (deduct)
                    // -------------------------
                    await conn.ExecuteAsync(@"
UPDATE dbo.ItemPrice
SET Qty = Qty - @Qty,
    IsTransfered = 1,
    UpdatedDate = GETDATE()
WHERE Id = @Id;
", new { Qty = t.TransferQty, Id = ip.Id }, tx);

                    // -------------------------
                    // 4) Update ItemWarehouseStock (source deduct)
                    // -------------------------
                    await conn.ExecuteAsync(@"
UPDATE dbo.ItemWarehouseStock
SET OnHand = OnHand - @Qty,
    Available = Available - @Qty,
    IsTransfered = 1,
    IsFullTransfer = @IsFull,
    IsPartialTransfer = @IsPartial
WHERE Id = @Id;
", new
                    {
                        Qty = t.TransferQty,
                        Id = ws.Id,
                        IsFull = isFull ? 1 : 0,
                        IsPartial = isPartial ? 1 : 0
                    }, tx);

                    // -------------------------
                    // 5) Update Stock row
                    // -------------------------
                    await conn.ExecuteAsync(@"
UPDATE dbo.Stock
SET OnHand = OnHand - @Qty,
    Available = Available - @Qty,
    TransferQty = ISNULL(TransferQty,0) - @Qty,
    Status = 2,
    Remarks = COALESCE(NULLIF(@Remarks,''), Remarks),
    UpdatedDate = GETDATE()
WHERE ID=@StockId AND ItemId=@ItemId;
", new
                    {
                        Qty = t.TransferQty,
                        t.StockId,
                        t.ItemId,
                        Remarks = t.Remarks
                    }, tx);

                    // =========================================================
                    // 6) Update MR Line ReceivedQty (MaterialReqId = MrId)
                    // =========================================================
                    int mrId = (t.MrId.GetValueOrDefault(0) > 0)
                        ? t.MrId.GetValueOrDefault(0)
                        : st.MrId;

                    if (mrId > 0)
                    {
                        var line = await conn.QueryFirstOrDefaultAsync<(int Id, decimal Qty, decimal ReceivedQty)>(@"
SELECT TOP 1 Id,
       CAST(Qty as decimal(18,4)) as Qty,
       CAST(ISNULL(ReceivedQty,0) as decimal(18,4)) as ReceivedQty
FROM dbo.MaterialRequisitionLine WITH (UPDLOCK, ROWLOCK)
WHERE MaterialReqId = @MrId
  AND ItemId = @ItemId
ORDER BY Id DESC;
", new { MrId = mrId, ItemId = t.ItemId }, tx);

                        if (line.Id == 0)
                            throw new Exception($"MR Line not found. MrId={mrId}, ItemId={t.ItemId}");

                        decimal newReceived = line.ReceivedQty + t.TransferQty;
                        if (newReceived > line.Qty) newReceived = line.Qty;

                        await conn.ExecuteAsync(@"
UPDATE dbo.MaterialRequisitionLine
SET ReceivedQty = @ReceivedQty
WHERE Id = @Id;
", new { ReceivedQty = newReceived, Id = line.Id }, tx);

                        touchedMrIds.Add(mrId);
                    }
                }

                // =========================================================
                // ✅ 7) Update MR Header Status:
                //    If ALL lines fully received => Status = 3
                //    Else => Status = 2
                // =========================================================
                if (touchedMrIds.Count > 0)
                {
                    var mrIdList = touchedMrIds.ToArray();

                    // For each MR: if any pending line exists => partial
                    var mrStatuses = await conn.QueryAsync<(int MrId, int HasPending)>(@"
SELECT 
    MaterialReqId AS MrId,
    CASE 
        WHEN SUM(CASE WHEN ISNULL(ReceivedQty,0) + 0.0000 < ISNULL(Qty,0) + 0.0000 THEN 1 ELSE 0 END) > 0 
            THEN 1 
        ELSE 0 
    END AS HasPending
FROM dbo.MaterialRequisitionLine WITH (UPDLOCK, ROWLOCK)
WHERE MaterialReqId IN @MrIds
GROUP BY MaterialReqId;
", new { MrIds = mrIdList }, tx);

                    foreach (var s in mrStatuses)
                    {
                        int newStatus = (s.HasPending == 1) ? 2 : 3;

                        await conn.ExecuteAsync(@"
UPDATE dbo.MaterialRequisition
SET Status = @Status,
    UpdatedDate = GETDATE()
WHERE Id = @MrId;
", new { Status = newStatus, MrId = s.MrId }, tx);
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











        public async Task<StockHistoryViewInfo> GetByIdStockHistory(long id)
        {

            const string query = @"
SELECT 
    s.ID,
    im.Id,
    im.Name,
    im.Sku,
    s.FromWarehouseID,
    whFrom.Name AS FromWarehouseName,
    s.ToWarehouseID,
    ISNULL(whTo.Name, 'Pending') AS ToWarehouseName,
    s.TransferQty,
    s.Available
FROM Stock AS s
INNER JOIN ItemMaster AS im 
    ON im.Id = s.ItemId
INNER JOIN ItemWarehouseStock AS iws 
    ON iws.ItemId = s.ItemId 
    AND iws.WarehouseId = s.FromWarehouseID   -- ✅ restrict to one warehouse
LEFT JOIN Warehouse AS whFrom 
    ON whFrom.Id = s.FromWarehouseID
LEFT JOIN Warehouse AS whTo 
    ON whTo.Id = s.ToWarehouseID
WHERE   s.Id = @Id ;
";
            return await Connection.QuerySingleAsync<StockHistoryViewInfo>(query, new { Id = id });
        }



        public async Task<IEnumerable<int>> GetTransferredMrIdsAsync()
        {
            const string sql = @"
SELECT DISTINCT CAST(MrId as int) AS MrId
FROM dbo.Stock
WHERE MrId IS NOT NULL AND MrId > 0;
";
            using var conn = Connection;
            return await conn.QueryAsync<int>(sql);
        }

        public async Task<IEnumerable<MaterialTransferListViewInfo>> GetMaterialTransferList()
        {
            const string query = @"
;WITH x AS (
    SELECT
        s.ID as StockId,
        s.ItemName,
        s.Sku,
        s.FromWarehouseID,
        s.FromWarehouseName,
        s.ToWarehouseID,
        s.ToBinId,
        s.OnHand,
        s.Available,
        s.MrId,
        s.SupplierId,
        s.BinId,
        s.BinName,
        s.Status,
        s.ItemId,

        wh.Name as ToWarehouseName,
        b.BinName as ToBinName,
        mr.ReqNo,

        mrl.Qty as OriginalRequestQty,
        mrl.ReceivedQty,

        -- ✅ current pending based on MR line
        CAST(mrl.Qty - ISNULL(mrl.ReceivedQty,0) AS decimal(18,4)) as PendingNow,

        sp.Name as SupplierName,

        -- ✅ include transfer qty for window calc
        CAST(ISNULL(s.TransferQty,0) AS decimal(18,4)) as TransferQty,
        ISNULL(s.isApproved, 0) as isApproved
    FROM stock as s
    inner join Warehouse as wh on wh.Id = s.ToWarehouseID
    inner join Bin as b on b.ID = s.ToBinId
    inner join MaterialRequisition mr on mr.Id = s.MrId

    -- ✅ IMPORTANT: match correct line (avoid duplicates)
    inner join MaterialRequisitionLine mrl 
        on mrl.MaterialReqId = mr.Id
       and mrl.ItemId = s.ItemId

    inner join Suppliers as sp on sp.Id = s.SupplierId
)
SELECT
    StockId, ItemName, Sku,
    FromWarehouseID, FromWarehouseName,
    ToWarehouseID, ToWarehouseName,
    ToBinId, ToBinName,
    OnHand, Available,
    MrId, ReqNo,
    SupplierId, SupplierName,
    BinId, BinName,
    Status, ItemId,
    OriginalRequestQty,
    ReceivedQty,

    -- ✅ Row-wise RequestQty (1st row 10, 2nd row 5)
    CAST(
        PendingNow
        + ISNULL(
            SUM(
                CASE 
                    WHEN isApproved = 1 AND TransferQty <> 0
                        THEN ABS(TransferQty)
                    ELSE 0
                END
            )
            OVER (
                PARTITION BY MrId, ItemId
                ORDER BY StockId
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
        ,0)
    AS decimal(18,4)) AS RequestQty
FROM x
ORDER BY MrId, ItemId, StockId;

";

            return await Connection.QueryAsync<MaterialTransferListViewInfo>(query);
        }



        public async Task<IEnumerable<MaterialTransferListViewInfo>> GetAllStockTransferedList()
        {
            const string query = @"
;WITH x AS (
    SELECT
        s.ID as StockId,
        s.ItemName,
        s.Sku,
        s.FromWarehouseID,
        s.FromWarehouseName,
        s.ToWarehouseID,
        s.ToBinId,
        s.OnHand,
        s.Available,
        s.MrId,
        s.SupplierId,
        s.BinId,
        s.BinName,
        s.Status,
        s.TransferQty,
        s.isApproved,
		s.ItemId,

        wh.Name as ToWarehouseName,
        b.BinName as ToBinName,
        mr.ReqNo,

        CAST(mrl.Qty - ISNULL(mrl.ReceivedQty,0) AS decimal(18,4)) as PendingNow,
        sp.Name as SupplierName
    FROM Stock s
    INNER JOIN Warehouse wh ON wh.Id = s.ToWarehouseID
    INNER JOIN Bin b ON b.ID = s.ToBinId
    INNER JOIN MaterialRequisition mr ON mr.Id = s.MrId

    -- ✅ IMPORTANT: join must match item (use sku/itemcode)
    INNER JOIN MaterialRequisitionLine mrl
        ON mrl.MaterialReqId = mr.Id
       AND mrl.ItemCode = s.Sku

    INNER JOIN Suppliers sp ON sp.Id = s.SupplierId
)
SELECT
    StockId, ItemName, Sku,
    FromWarehouseID, FromWarehouseName,
    ToWarehouseID, ToWarehouseName,
    ToBinId, ToBinName,
    OnHand, Available,
    MrId, ReqNo,
    SupplierId, SupplierName,
    BinId, BinName,
    Status, TransferQty,itemId,

    -- ✅ Row-wise RequestQty:
    CAST(
        PendingNow
        + ISNULL(
            SUM(CASE WHEN isApproved = 1 AND TransferQty IS NOT NULL
                     THEN ABS(CAST(TransferQty AS decimal(18,4)))
                     ELSE 0 END)
            OVER (
                PARTITION BY MrId, Sku
                ORDER BY StockId
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
        ,0)
    AS decimal(18,4)) AS RequestQty
FROM x
ORDER BY MrId, Sku, StockId;


";
            return await Connection.QueryAsync<MaterialTransferListViewInfo>(query);
        }
    }
}
