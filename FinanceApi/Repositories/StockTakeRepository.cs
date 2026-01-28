using System.Data;
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.Data.SqlClient;

namespace FinanceApi.Repositories
{
    public class StockTakeRepository : DynamicRepository, IStockTakeRepository
    {
        private readonly ApplicationDbContext _context;

        public StockTakeRepository(IDbConnectionFactory connectionFactory)
        : base(connectionFactory)
        {

        }

        public async Task<IEnumerable<StockTakeDTO>> GetAllAsync()
        {
            const string headersSql = @"
        SELECT
            st.Id,
            --st.TakeTypeId,
            st.WarehouseTypeId,
            ISNULL(w.Name,'') AS WarehouseName,
            st.StrategyId,
            ISNULL(s.StrategyName,'') AS StrategyName,
            st.SupplierId,
            ISNULL(sp.Name,'') AS SupplierName,
            st.Freeze,
            st.Status,
            st.CreatedBy,
            st.CreatedDate,
            st.UpdatedBy,
            st.UpdatedDate,
            st.IsActive
        FROM StockTake st
        LEFT JOIN Warehouse w ON st.WarehouseTypeId = w.Id
        LEFT JOIN Strategy  s ON st.StrategyId      = s.Id
        LEFT JOIN Suppliers  sp ON st.SupplierId      = sp.Id
        WHERE st.IsActive = 1
        ORDER BY st.Id;";

            // 1) Load headers
            var headers = (await Connection.QueryAsync<StockTakeDTO>(headersSql)).ToList();
            if (headers.Count == 0) return headers;

            // 2) Load all lines for these headers
            var ids = headers.Select(h => h.Id).ToArray();
            const string linesSql = @"
        SELECT
            Id,
            StockTakeId,
            WarehouseTypeId,
            SupplierId,
            Status,
            ItemId,
            BinId,
            OnHand,
            CountedQty,
            BadCountedQty,
            VarianceQty,
            Barcode,
            ReasonId,
            Remarks,
            Selected,
            CreatedBy,
            CreatedDate,
            UpdatedBy,
            UpdatedDate,
            IsActive
        FROM StockTakeLines
        WHERE StockTakeId IN @Ids AND IsActive = 1;";

            var lines = await Connection.QueryAsync<StockTakeLines>(linesSql, new { Ids = ids });

            // 3) Attach
            var byId = headers.ToDictionary(h => h.Id);
            foreach (var ln in lines)
            {
                if (byId.TryGetValue(ln.StockTakeId, out var parent))
                    parent.LineItems.Add(ln);
            }

            return headers;
        }

        public async Task<IEnumerable<SupplierDto>> GetAllSupplierByWarehouseIdAsync(int id)
        {

            const string sql = @"
        SELECT DISTINCT
            ip.SupplierId AS Id,
            sp.Name
        FROM dbo.ItemPrice ip
        INNER JOIN dbo.Suppliers sp ON sp.Id = ip.SupplierId
        WHERE ip.WarehouseId = @WarehouseId
        ORDER BY sp.Name;";

            var rows = await Connection.QueryAsync<SupplierDto>(sql, new { WarehouseId = id });
            return rows.AsList();

        }

        public async Task<IEnumerable<StockTakeWarehouseItem>> GetWarehouseItemsAsync(
     long warehouseId,
     long supplierId,        // pass 0 for "no supplier filter"
     long? strategyId)
        {
            // ✅ Supplier = ALL : don't use findStockTakeSql / postedSql
            if (supplierId == 0)
            {
                const string baseAllSuppliersSql = @" 
;WITH BaseItems AS (
    SELECT
        im.Id                     AS ItemId,
        im.Sku                    AS Sku,
        im.Name                   AS ItemName,
        iws.WarehouseId,
        w.Name                    AS WarehouseName,
        iws.BinId,
        b.BinName                 AS BinName,
        CAST(NULL AS varchar(50)) AS BinCode,

        ip.SupplierId             AS SupplierId,
        sp.Name                   AS SupplierName,

        ISNULL(iws.OnHand,0)      AS WarehouseOnHand,
        ISNULL(ip.Qty,0)          AS OnHand,
        ISNULL(ip.Qty,0)          AS CurrentSupplierQty,
        ISNULL(ip.BadCountedQty,0) AS BadOnHand,
        CAST(0 AS decimal(18,3))  AS Reserved,
        ISNULL(ip.Qty,0)          AS AvailableQty,

        iws.MinQty,
        iws.MaxQty,
        iws.ReorderQty
    FROM dbo.ItemWarehouseStock iws
    JOIN dbo.ItemMaster im ON im.Id = iws.ItemId
    JOIN dbo.Warehouse  w  ON w.Id  = iws.WarehouseId
    LEFT JOIN dbo.Bin   b  ON b.Id  = iws.BinId
    JOIN dbo.ItemPrice ip
      ON ip.ItemId      = iws.ItemId
     AND ip.WarehouseId = iws.WarehouseId
     AND ISNULL(ip.Qty,0) > 0
    LEFT JOIN dbo.Suppliers sp ON sp.Id = ip.SupplierId
    WHERE
        iws.WarehouseId = @WarehouseId
        AND (NULLIF(@StrategyId,0) IS NULL OR iws.StrategyId = @StrategyId)
),
OpenTakePerSupplier AS (
    -- if supplier has Draft/Approved take -> hide in ALL mode
    SELECT SupplierId
    FROM (
        SELECT
            COALESCE(NULLIF(st.SupplierId,0), stl.SupplierId) AS SupplierId,
            ROW_NUMBER() OVER (
              PARTITION BY COALESCE(NULLIF(st.SupplierId,0), stl.SupplierId)
              ORDER BY st.CreatedDate DESC, st.Id DESC
            ) AS rn
        FROM dbo.StockTake st
        LEFT JOIN dbo.StockTakeLines stl
          ON stl.StockTakeId = st.Id AND stl.IsActive = 1
        WHERE st.IsActive = 1
          AND st.WarehouseTypeId = @WarehouseId
          AND (NULLIF(@StrategyId,0) IS NULL OR st.StrategyId = @StrategyId)
          AND st.Status IN (1,2)
          AND (st.SupplierId <> 0 OR stl.SupplierId IS NOT NULL)
    ) x
    WHERE rn = 1
),
PostedTakes AS (
    -- all posted takes in this warehouse/strategy
    SELECT st.Id AS StockTakeId
    FROM dbo.StockTake st
    WHERE st.IsActive = 1
      AND st.Status = 3
      AND st.WarehouseTypeId = @WarehouseId
      AND (NULLIF(@StrategyId,0) IS NULL OR st.StrategyId = @StrategyId)
),
BestPostedLine AS (
    -- latest posted line per (Supplier, Item, Bin) across ALL posted takes
    SELECT *
    FROM (
        SELECT
            l.Id,
            l.SupplierId,
            l.ItemId,
            l.BinId,
            ISNULL(l.Selected,0)    AS Selected,
            ISNULL(l.VarianceQty,0) AS VarianceQty,
            l.OnHand                AS LineOnHand,
            ROW_NUMBER() OVER (
                PARTITION BY l.SupplierId, l.ItemId, ISNULL(l.BinId,0)
                ORDER BY l.StockTakeId DESC, l.Id DESC
            ) AS rn
        FROM dbo.StockTakeLines l
        JOIN PostedTakes pt ON pt.StockTakeId = l.StockTakeId
        WHERE l.IsActive = 1
    ) x
    WHERE rn = 1
),
Joined AS (
    SELECT
        bi.*,
        pl.Id         AS LineId,
        pl.Selected   AS Selected,
        pl.VarianceQty AS VarianceQty,
        pl.LineOnHand AS LineOnHand
    FROM BaseItems bi
    LEFT JOIN BestPostedLine pl
      ON pl.SupplierId = bi.SupplierId
     AND pl.ItemId     = bi.ItemId
     AND ISNULL(pl.BinId,0) = ISNULL(bi.BinId,0)
)
SELECT
    ItemId, Sku, ItemName,
    WarehouseId, WarehouseName,
    BinId, BinName, BinCode,
    SupplierId, SupplierName,
    OnHand,BadOnHand, Reserved, AvailableQty,
    MinQty, MaxQty, ReorderQty
FROM Joined
WHERE
    -- hide suppliers that currently have Draft/Approved stocktake
    SupplierId NOT IN (SELECT SupplierId FROM OpenTakePerSupplier)
    AND (
        -- never posted before (no line)
        LineId IS NULL
        -- posted before but needs action
        OR ISNULL(Selected,0) = 0
        OR ISNULL(VarianceQty,0) <> 0
        OR ISNULL(CurrentSupplierQty,0) <> ISNULL(LineOnHand,0)

    )
ORDER BY Sku, ItemName, SupplierName;

  ";

                return await Connection.QueryAsync<StockTakeWarehouseItem>(baseAllSuppliersSql, new
                {
                    WarehouseId = warehouseId,
                    StrategyId = strategyId
                });
            }

            // 1) Find the most relevant matching StockTake (if any)
            const string findStockTakeSql = @"
SELECT TOP (1)
    st.Id     AS StockTakeId,
    st.Status AS StockTakeStatus
FROM dbo.StockTake st
WHERE
    st.IsActive = 1
    AND st.WarehouseTypeId = @WarehouseId
    AND (NULLIF(@StrategyId,0) IS NULL OR st.StrategyId = @StrategyId)
    AND (
        -- supplier-specific header
        st.SupplierId = @SupplierId

        -- OR supplier exists via lines (covers ALL header SupplierId=0)
        OR EXISTS (
            SELECT 1
            FROM dbo.StockTakeLines l
            WHERE l.StockTakeId = st.Id
              AND l.IsActive = 1
              AND l.SupplierId = @SupplierId
        )
    )
ORDER BY st.CreatedDate DESC, st.Id DESC;
";

            var match = await Connection.QueryFirstOrDefaultAsync<(long StockTakeId, int StockTakeStatus)>(
                findStockTakeSql,
                new { WarehouseId = warehouseId, SupplierId = supplierId, StrategyId = strategyId }
            );

            // 2) If matched & status is 1 or 2 (Draft/Approve), throw an error
            if (supplierId != 0 &&  match.StockTakeId != 0 && (match.StockTakeStatus == 1 || match.StockTakeStatus == 2))
            {
                // throw a meaningful error; adjust exception type to your API layer
                throw new InvalidOperationException(
                    "A matching StockTake exists in Draft/Approved status.So Listing items is not allowed.Check and Proceed");
            }

            // 3) If NO match → return the normal list (BaseItems)
            if (match.StockTakeId == 0)
            {
                const string baseAllSuppliersSql = @"
;WITH BaseItems AS (
    SELECT
        im.Id                     AS ItemId,
        im.Sku                    AS Sku,
        im.Name                   AS ItemName,
        iws.WarehouseId,
        w.Name                    AS WarehouseName,
        iws.BinId,
        b.BinName                 AS BinName,
        CAST(NULL AS varchar(50)) AS BinCode,

        ip.SupplierId             AS SupplierId,
        sp.Name                   AS SupplierName,

        ISNULL(iws.OnHand,0)      AS WarehouseOnHand,
        ISNULL(ip.Qty,0)          AS OnHand,
        ISNULL(ip.BadCountedQty,0) AS BadOnHand,
        ISNULL(ip.Qty,0)          AS CurrentSupplierQty,   -- ✅ NEW (used for compare)
        CAST(0 AS decimal(18,3))  AS Reserved,
        ISNULL(ip.Qty,0)          AS AvailableQty,

        iws.MinQty,
        iws.MaxQty,
        iws.ReorderQty
    FROM dbo.ItemWarehouseStock iws
    JOIN dbo.ItemMaster im ON im.Id = iws.ItemId
    JOIN dbo.Warehouse  w  ON w.Id  = iws.WarehouseId
    LEFT JOIN dbo.Bin   b  ON b.Id  = iws.BinId
    JOIN dbo.ItemPrice ip
      ON ip.ItemId = iws.ItemId
     AND ip.WarehouseId = iws.WarehouseId
     AND ISNULL(ip.Qty,0) > 0
    LEFT JOIN dbo.Suppliers sp ON sp.Id = ip.SupplierId
    WHERE
        iws.WarehouseId = @WarehouseId
        AND (NULLIF(@StrategyId,0) IS NULL OR iws.StrategyId = @StrategyId)
),
TakeSupplier AS (
    -- ✅ Map each stocktake to suppliers using header OR lines (covers header SupplierId=0)
    SELECT DISTINCT
        st.Id,
        st.Status,
        st.CreatedDate,
        COALESCE(NULLIF(st.SupplierId,0), stl.SupplierId) AS SupplierId
    FROM dbo.StockTake st
    LEFT JOIN dbo.StockTakeLines stl
      ON stl.StockTakeId = st.Id
     AND stl.IsActive = 1
    WHERE
        st.IsActive = 1
        AND st.WarehouseTypeId = @WarehouseId
        AND (NULLIF(@StrategyId,0) IS NULL OR st.StrategyId = @StrategyId)
        AND (st.SupplierId <> 0 OR stl.SupplierId IS NOT NULL)
),
LatestOpenTake AS (
    -- ✅ Latest Draft/Approved per supplier
    SELECT SupplierId, StockTakeId
    FROM (
        SELECT
            ts.SupplierId,
            ts.Id AS StockTakeId,
            ROW_NUMBER() OVER (PARTITION BY ts.SupplierId ORDER BY ts.CreatedDate DESC, ts.Id DESC) AS rn
        FROM TakeSupplier ts
        WHERE ts.Status IN (1,2)
    ) x
    WHERE rn = 1
),
LatestPostedTake AS (
    -- ✅ Latest Posted per supplier
    SELECT SupplierId, StockTakeId
    FROM (
        SELECT
            ts.SupplierId,
            ts.Id AS StockTakeId,
            ROW_NUMBER() OVER (PARTITION BY ts.SupplierId ORDER BY ts.CreatedDate DESC, ts.Id DESC) AS rn
        FROM TakeSupplier ts
        WHERE ts.Status = 3
    ) x
    WHERE rn = 1
),
Joined AS (
    SELECT
        bi.*,
        ot.StockTakeId AS OpenTakeId,
        pt.StockTakeId AS PostedTakeId,

        stl.Id                     AS LineId,
        ISNULL(stl.Selected,0)     AS Selected,
        ISNULL(stl.VarianceQty,0)  AS VarianceQty,
        stl.OnHand                 AS LineOnHand
    FROM BaseItems bi
    LEFT JOIN LatestOpenTake ot
      ON ot.SupplierId = bi.SupplierId
    LEFT JOIN LatestPostedTake pt
      ON pt.SupplierId = bi.SupplierId
    LEFT JOIN dbo.StockTakeLines stl
      ON stl.StockTakeId = pt.StockTakeId
     AND stl.IsActive = 1
     AND stl.SupplierId = bi.SupplierId
    AND stl.ItemId = bi.ItemId
AND ISNULL(stl.BinId,0) = ISNULL(bi.BinId,0)
)
SELECT
    ItemId, Sku, ItemName,
    WarehouseId, WarehouseName,
    BinId, BinName, BinCode,
    SupplierId, SupplierName,
    OnHand,BadOnHand, Reserved, AvailableQty,
    MinQty, MaxQty, ReorderQty
FROM Joined
WHERE
    OpenTakeId IS NULL
    AND (
        PostedTakeId IS NULL
        OR ISNULL(Selected,0) = 0
        OR ISNULL(VarianceQty,0) <> 0
        OR ISNULL(WarehouseOnHand,0) <> ISNULL(LineOnHand,0)
    )
    AND NOT (
        LineId IS NOT NULL
        AND ISNULL(Selected,0) = 1
        AND ISNULL(VarianceQty,0) = 0
        AND ISNULL(WarehouseOnHand,0) = ISNULL(LineOnHand,0)
    )

ORDER BY Sku, ItemName, SupplierName;


";

                const string baseSingleSupplierSql = @"
SELECT
    im.Id                      AS ItemId,
    im.Sku                     AS Sku,
    im.Name                    AS ItemName,
    iws.WarehouseId,
    w.Name                     AS WarehouseName,
    iws.BinId,
    b.BinName                  AS BinName,
    CAST(NULL AS varchar(50))  AS BinCode,

    ISNULL(ip.Qty, 0)          AS OnHand,
    ISNULL(ip.BadCountedQty,0) AS BadOnHand,
    CAST(0 AS decimal(18,3))   AS Reserved,
    ISNULL(ip.Qty, 0)          AS AvailableQty,

    iws.MinQty,
    iws.MaxQty,
    iws.ReorderQty,

    @SupplierId                AS SupplierId,
    sp.Name                    AS SupplierName
FROM dbo.ItemWarehouseStock iws
JOIN dbo.ItemMaster im ON im.Id = iws.ItemId
JOIN dbo.Warehouse  w  ON w.Id  = iws.WarehouseId
LEFT JOIN dbo.Bin   b  ON b.Id  = iws.BinId
LEFT JOIN dbo.ItemPrice ip
  ON ip.ItemId      = iws.ItemId
 AND ip.WarehouseId = iws.WarehouseId
 AND ip.SupplierId  = @SupplierId
LEFT JOIN dbo.Suppliers sp ON sp.Id = @SupplierId
WHERE
    iws.WarehouseId = @WarehouseId
    AND (NULLIF(@StrategyId,0) IS NULL OR iws.StrategyId = @StrategyId)
    AND ISNULL(ip.Qty, 0) > 0
ORDER BY im.Sku, im.Name;";

                var sql = (supplierId == 0) ? baseAllSuppliersSql : baseSingleSupplierSql;

                return await Connection.QueryAsync<StockTakeWarehouseItem>(sql, new
                {
                    WarehouseId = warehouseId,
                    SupplierId = supplierId,
                    StrategyId = strategyId
                });
            }



            // 4) Matched & Status = 3 → apply your three conditions, supplier-aware
            const string postedSql = @"
;WITH BaseItems AS (
    SELECT
        im.Id                     AS ItemId,
        im.Sku                    AS Sku,
        im.Name                   AS ItemName,
        iws.WarehouseId,
        w.Name                    AS WarehouseName,
        iws.BinId,
        b.BinName                 AS BinName,
        CAST(NULL AS varchar(50)) AS BinCode,

        @SupplierId               AS SupplierId,
        ISNULL(sp.Name,'')        AS SupplierName,

        ISNULL(ip.Qty,0)          AS OnHand,
        ISNULL(ip.BadCountedQty,0) AS BadOnHand,
        ISNULL(ip.Qty,0)          AS CurrentSupplierQty,
        CAST(0 AS decimal(18,3))  AS Reserved,
        ISNULL(ip.Qty,0)          AS AvailableQty,

        iws.MinQty,
        iws.MaxQty,
        iws.ReorderQty
    FROM dbo.ItemWarehouseStock iws
    JOIN dbo.ItemMaster im ON im.Id = iws.ItemId
    JOIN dbo.Warehouse  w  ON w.Id  = iws.WarehouseId
    LEFT JOIN dbo.Bin   b  ON b.Id  = iws.BinId
    JOIN dbo.ItemPrice ip
      ON ip.ItemId      = iws.ItemId
     AND ip.WarehouseId = iws.WarehouseId
     AND ip.SupplierId  = @SupplierId
     AND ISNULL(ip.Qty,0) > 0
    LEFT JOIN dbo.Suppliers sp ON sp.Id = @SupplierId
    WHERE
        iws.WarehouseId = @WarehouseId
        AND (NULLIF(@StrategyId,0) IS NULL OR iws.StrategyId = @StrategyId)
),
PostedTakes AS (
    -- ✅ ALL posted stocktakes for this supplier+warehouse+strategy (includes old 6 and new 11)
    SELECT st.Id AS StockTakeId
    FROM dbo.StockTake st
    WHERE st.IsActive = 1
      AND st.Status = 3
      AND st.WarehouseTypeId = @WarehouseId
      AND (NULLIF(@StrategyId,0) IS NULL OR st.StrategyId = @StrategyId)
      AND (
            st.SupplierId = @SupplierId
            OR EXISTS (
                SELECT 1
                FROM dbo.StockTakeLines l
                WHERE l.StockTakeId = st.Id
                  AND l.IsActive = 1
                  AND l.SupplierId = @SupplierId
            )
      )
),
BestPostedLine AS (
    -- ✅ For each (Supplier, Item, Bin) pick the latest posted line among ALL posted takes
    SELECT *
    FROM (
        SELECT
            l.Id,
            l.ItemId,
            l.BinId,
            l.SupplierId,
            ISNULL(l.Selected,0)    AS Selected,
            ISNULL(l.VarianceQty,0) AS VarianceQty,
            l.OnHand                AS LineOnHand,
            ROW_NUMBER() OVER (
                PARTITION BY l.SupplierId, l.ItemId, ISNULL(l.BinId,-1)
                ORDER BY l.StockTakeId DESC, l.Id DESC
            ) AS rn
        FROM dbo.StockTakeLines l
        JOIN PostedTakes pt ON pt.StockTakeId = l.StockTakeId
        WHERE l.IsActive = 1
          AND l.SupplierId = @SupplierId
    ) x
    WHERE rn = 1
),
Joined AS (
    SELECT
        bi.*,
        pl.Id         AS LineId,
        pl.Selected   AS Selected,
        pl.VarianceQty AS VarianceQty,
        pl.LineOnHand AS LineOnHand
    FROM BaseItems bi
    LEFT JOIN BestPostedLine pl
      ON pl.SupplierId = bi.SupplierId
     AND pl.ItemId = bi.ItemId
     AND ( (pl.BinId = bi.BinId) OR (pl.BinId IS NULL AND bi.BinId IS NULL) )
)
SELECT
    ItemId, Sku, ItemName,
    WarehouseId, WarehouseName,
    BinId, BinName, BinCode,
    SupplierId, SupplierName,
    OnHand,BadOnHand, Reserved, AvailableQty,
    MinQty, MaxQty, ReorderQty,
    Selected, VarianceQty, LineOnHand,

    CASE
      WHEN LineId IS NULL THEN 1
      WHEN ISNULL(Selected,0) = 0 THEN 1
      WHEN ISNULL(VarianceQty,0) <> 0 THEN 1
      WHEN ISNULL(CurrentSupplierQty,0) <> ISNULL(LineOnHand,0) THEN 1
      ELSE 0
    END AS NeedsAction,

    CASE
      WHEN LineId IS NOT NULL
       AND Selected = 1
       AND ISNULL(VarianceQty,0) = 0
       AND ISNULL(CurrentSupplierQty,0) = ISNULL(LineOnHand,0)
      THEN 1 ELSE 0
    END AS AlreadyCheckedPosted
FROM Joined
ORDER BY Sku, ItemName, SupplierName;
";


            var rows = (await Connection.QueryAsync<StockTakeWarehouseItem>(
    postedSql,
    new
    {
        WarehouseId = warehouseId,
        SupplierId = supplierId,  // pass 0 for 'no supplier filter'
        StrategyId = strategyId,
        StockTakeId = match.StockTakeId
    })
).ToList();

            // Count items that are already checked & posted
            int already = rows.Count(r => r.AlreadyCheckedPosted == 1);

            // If nothing needs action => show error
            if (rows.Count > 0 && rows.All(r => r.NeedsAction == 0))
            {
                throw new InvalidOperationException(
                    "All selected lines for this supplier in this warehouse are already checked & posted..");
            }

            // Return only lines that need action
            return rows.Where(r => r.NeedsAction == 1).ToList();

            return rows;


        }



        public async Task<StockTakeDTO?> GetByIdAsync(int id)
        {
            const string headerSql = @"
        SELECT TOP (1)
            st.Id,
            --st.TakeTypeId,
            st.WarehouseTypeId,
            ISNULL(w.Name,'') AS WarehouseName,
            st.StrategyId,
            ISNULL(s.StrategyName,'') AS StrategyName,
            st.SupplierId,
            ISNULL(sp.Name,'') AS SupplierName,
            st.Freeze,
            st.Status,
            st.CreatedBy,
            st.CreatedDate,
            st.UpdatedBy,
            st.UpdatedDate,
            st.IsActive
        FROM StockTake st
        LEFT JOIN Warehouse w ON st.WarehouseTypeId = w.Id
        LEFT JOIN Strategy  s ON st.StrategyId      = s.Id
        LEFT JOIN Suppliers  sp ON st.SupplierId      = sp.Id
        WHERE st.Id = @Id AND st.IsActive = 1;";

            // 1) header
            var header = await Connection.QueryFirstOrDefaultAsync<StockTakeDTO>(headerSql, new { Id = id });
            if (header == null) return null;

            const string linesSql = @"
   SELECT
    stl.Id,
    stl.StockTakeId,
    stl.WarehouseTypeId,
    stl.SupplierId,
    ISNULL(sp.Name,'') AS SupplierName,

    stl.Status,
    stl.ItemId,
    stl.BinId,

    stl.OnHand,
    ISNULL(ip.BadOnHand,0) AS BadOnHand,      -- ✅ book faulty qty

    stl.CountedQty,
    stl.BadCountedQty,
    stl.VarianceQty,
    stl.Barcode,
    stl.ReasonId,
    stl.Remarks,
    stl.Selected,
    stl.CreatedBy,
    stl.CreatedDate,
    stl.UpdatedBy,
    stl.UpdatedDate,
    stl.IsActive
FROM StockTakeLines stl
LEFT JOIN Suppliers sp ON sp.Id = stl.SupplierId

OUTER APPLY (
    SELECT TOP(1)
        ISNULL(ip.BadCountedQty,0) AS BadOnHand
    FROM dbo.ItemPrice ip
    WHERE ip.ItemId = stl.ItemId
      AND ip.SupplierId = stl.SupplierId
      AND ip.WarehouseId = stl.WarehouseTypeId
    ORDER BY ip.Id DESC
) ip

WHERE stl.StockTakeId = @Id AND stl.IsActive = 1
ORDER BY stl.Id;

";

            // 2) lines
            var lines = await Connection.QueryAsync<StockTakeLines>(linesSql, new { Id = id });
            header.LineItems = lines.ToList();

            return header;
        }

        public async Task<int> CreateAsync(StockTake stockTake)
        {
            if (stockTake is null) throw new ArgumentNullException(nameof(stockTake));

            var now = DateTime.UtcNow;
            if (stockTake.CreatedDate == default) stockTake.CreatedDate = now;
            if (stockTake.UpdatedDate == default) stockTake.UpdatedDate = now;

            const string insertHeaderSql = @"
INSERT INTO StockTake
(
    WarehouseTypeId,StrategyId,SupplierId,
    Freeze, Status, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
OUTPUT INSERTED.Id
VALUES
(
    @WarehouseTypeId,@StrategyId,@SupplierId,
    @Freeze, @Status, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive
);";

            const string insertLinesSql = @"
INSERT INTO StockTakeLines
(
    StockTakeId, ItemId,BinId, OnHand, CountedQty,BadCountedQty, VarianceQty,ReasonId,WarehouseTypeId,SupplierId,Status,
    Barcode, Remarks,Selected, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
VALUES
(
    @StockTakeId, @ItemId,@BinId, @OnHand, @CountedQty,@BadCountedQty, @VarianceQty,@ReasonId,@WarehouseTypeId,@SupplierId,@Status,
    @Barcode, @Remarks,@Selected, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive
);";

            // ✅ HOLD the same connection instance for the whole method
            var conn = Connection;                             // capture once
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();    // or conn.Open() if you prefer sync

            using var tx = conn.BeginTransaction();            // ✅ same instance

            try
            {
                // 1) Insert header
                var newId = await conn.ExecuteScalarAsync<int>(
                    insertHeaderSql,
                    new
                    {
                        stockTake.WarehouseTypeId,                       
                        //stockTake.TakeTypeId,
                        stockTake.StrategyId,
                        stockTake.SupplierId,
                        stockTake.Freeze,
                        stockTake.Status,
                        stockTake.CreatedBy,
                        stockTake.CreatedDate,
                        stockTake.UpdatedBy,
                        stockTake.UpdatedDate,
                        IsActive = stockTake.IsActive
                    },
                    transaction: tx
                );

                // 2) Insert lines (if any)
                if (stockTake.LineItems?.Count > 0)
                {
                    var lineParams = stockTake.LineItems.Select(l => new
                    {
                        StockTakeId = newId,
                        l.ItemId,
                        l.BinId,
                        l.WarehouseTypeId,
                        l.SupplierId,
                        l.Status,
                        l.OnHand,
                        CountedQty = l.CountedQty,
                        BadCountedQty = l.BadCountedQty,
                        VarianceQty = (l.CountedQty.HasValue || l.BadCountedQty.HasValue)
    ? ((l.CountedQty ?? 0m) + (l.BadCountedQty ?? 0m)) - l.OnHand
    : (decimal?)null,
                        l.ReasonId,
                        l.Barcode,
                        l.Remarks,
                        l.Selected,
                        CreatedBy = stockTake.CreatedBy,
                        CreatedDate = stockTake.CreatedDate,
                        UpdatedBy = stockTake.UpdatedBy,
                        UpdatedDate = stockTake.UpdatedDate,
                        IsActive = true
                    });

                    await conn.ExecuteAsync(insertLinesSql, lineParams, transaction: tx);
                }

                tx.Commit();
                return newId;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }




        public async Task UpdateAsync(StockTake updatedStockTake)
        {
            if (updatedStockTake is null) throw new ArgumentNullException(nameof(updatedStockTake));

            var now = DateTime.UtcNow;
            updatedStockTake.UpdatedDate = now;

            const string updateHeaderSql = @"
UPDATE StockTake
SET
    WarehouseTypeId = @WarehouseTypeId,
    --TakeTypeId      = @TakeTypeId,   -- if you renamed: TakeType
    StrategyId      = @StrategyId,
    SupplierId      = @SupplierId,
    Freeze          = @Freeze,
    Status          = @Status,
    UpdatedBy       = @UpdatedBy,
    UpdatedDate     = @UpdatedDate
WHERE Id = @Id;";

            const string updateLineSql = @"
UPDATE StockTakeLines
SET
    ItemId      = @ItemId,
    BinId       = @BinId,
    WarehouseTypeId = @WarehouseTypeId, 
    SupplierId = @SupplierId,
    Status  = @Status,
    OnHand      = @OnHand,
    CountedQty  = @CountedQty,
    BadCountedQty  = @BadCountedQty,
    VarianceQty = @VarianceQty,
    ReasonId = @ReasonId,
    Barcode     = @Barcode,
    Remarks     = @Remarks,
    Selected = @Selected,
    UpdatedBy   = @UpdatedBy,
    UpdatedDate = @UpdatedDate,
    IsActive    = 1
WHERE Id = @Id AND StockTakeId = @StockTakeId;";

            const string insertLineSql = @"
INSERT INTO StockTakeLines
(
    StockTakeId, ItemId,BinId, OnHand, CountedQty, BadCountedQty,VarianceQty,ReasonId,WarehouseTypeId,SupplierId,Status,
    Barcode, Remarks,Selected, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
VALUES
(
    @StockTakeId, @ItemId,@BinId, @OnHand, @CountedQty, @BadCountedQty,@VarianceQty,@ReasonId,@WarehouseTypeId,@SupplierId,@Status,
    @Barcode, @Remarks,@Selected, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1
);
SELECT CAST(SCOPE_IDENTITY() AS INT);";

            const string softDeleteMissingSql = @"
UPDATE StockTakeLines
SET IsActive = 0, UpdatedBy = @UpdatedBy, UpdatedDate = @UpdatedDate
WHERE StockTakeId = @StockTakeId
  AND IsActive = 1
  AND (@KeepIdsCount = 0 OR Id NOT IN @KeepIds);";

            // ✅ Capture the SAME connection instance and open it
            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();

            try
            {
                // 1) Update header
                await conn.ExecuteAsync(updateHeaderSql, new
                {
                    updatedStockTake.WarehouseTypeId,                  
                    //updatedStockTake.TakeTypeId,   // if renamed -> TakeType
                    updatedStockTake.StrategyId,
                    updatedStockTake.SupplierId,
                    updatedStockTake.Freeze,
                    updatedStockTake.Status,
                    updatedStockTake.UpdatedBy,
                    updatedStockTake.UpdatedDate,
                    updatedStockTake.Id
                }, tx);

                // 2) Upsert lines
                var keepIds = new List<int>();

                if (updatedStockTake.LineItems?.Count > 0)
                {
                    foreach (var l in updatedStockTake.LineItems)
                    {
                        decimal? variance =
    (l.CountedQty.HasValue || l.BadCountedQty.HasValue)
        ? ((l.CountedQty ?? 0m) + (l.BadCountedQty ?? 0m)) - l.OnHand
        : (decimal?)null;

                        if (l.Id > 0)
                        {
                            await conn.ExecuteAsync(updateLineSql, new
                            {
                                Id = l.Id,
                                StockTakeId = updatedStockTake.Id,
                                l.ItemId,
                                l.BinId,
                                l.WarehouseTypeId,
                                l.SupplierId,
                                l.Status,
                                l.OnHand,
                                CountedQty = l.CountedQty,
                                BadCountedQty = l.BadCountedQty,
                                VarianceQty = variance,
                                l.ReasonId,
                                l.Barcode,
                                l.Remarks,
                                l.Selected,
                                UpdatedBy = updatedStockTake.UpdatedBy,
                                UpdatedDate = now
                            }, tx);

                            keepIds.Add(l.Id);
                        }
                        else
                        {
                            var newLineId = await conn.ExecuteScalarAsync<int>(insertLineSql, new
                            {
                                StockTakeId = updatedStockTake.Id,
                                l.ItemId,
                                l.BinId,
                                l.WarehouseTypeId,
                                l.SupplierId,
                                l.Status,
                                l.OnHand,
                                CountedQty = l.CountedQty,
                                BadCountedQty = l.BadCountedQty,
                                VarianceQty = variance,
                                l.ReasonId,
                                l.Barcode,
                                l.Remarks,
                                l.Selected,
                                CreatedBy = updatedStockTake.UpdatedBy ?? updatedStockTake.CreatedBy,
                                CreatedDate = now,
                                UpdatedBy = updatedStockTake.UpdatedBy,
                                UpdatedDate = now
                            }, tx);

                            keepIds.Add(newLineId);
                        }
                    }
                }

                // 3) Soft delete lines not sent from UI
                var keepIdsParam = keepIds.Count == 0 ? new[] { -1 } : keepIds.ToArray();

                await conn.ExecuteAsync(softDeleteMissingSql, new
                {
                    StockTakeId = updatedStockTake.Id,
                    KeepIds = keepIdsParam,       // Dapper expands for IN (@KeepIds)
                    KeepIdsCount = keepIds.Count,
                    UpdatedBy = updatedStockTake.UpdatedBy,
                    UpdatedDate = now
                }, tx);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }






        public async Task DeactivateAsync(int id, int updatedBy)
        {
            const string sqlHeader = @"
UPDATE StockTake
SET IsActive = 0, UpdatedBy = @UpdatedBy, UpdatedDate = SYSUTCDATETIME()
WHERE Id = @Id;";

            const string sqlLines = @"
UPDATE StockTakeLines
SET IsActive = 0, UpdatedBy = @UpdatedBy, UpdatedDate = SYSUTCDATETIME()
WHERE StockTakeId = @Id AND IsActive = 1;";

            // capture the same connection instance
            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();

            try
            {
                // optional: block if already posted/final
                // var status = await conn.ExecuteScalarAsync<int>("SELECT Status FROM StockTake WHERE Id=@Id", new { Id = id }, tx);
                // if (status == (int)StockTakeStatus.Posted) throw new InvalidOperationException("Cannot deactivate a posted stock take.");

                var affectedHeader = await conn.ExecuteAsync(sqlHeader, new { Id = id, UpdatedBy = updatedBy }, tx);
                if (affectedHeader == 0)
                    throw new KeyNotFoundException("StockTake not found.");

                await conn.ExecuteAsync(sqlLines, new { Id = id, UpdatedBy = updatedBy }, tx);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // In StockTakeRepository

        public async Task<int> CreateFromStockTakeAsync(
      int stockTakeId,
      string? reason,
      string? remarks,
      string userName,
      bool applyToStock = true,
      bool markPosted = true,
      DateTime? txnDateOverride = null,
      bool onlySelected = true)
        {
            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {
                // 1) Header
                const string headerSql = @"
SELECT TOP(1)
    Id, WarehouseTypeId, StrategyId, SupplierId, Freeze, Status, IsActive
FROM StockTake
WHERE Id = @Id AND IsActive = 1;";
                var header = await conn.QueryFirstOrDefaultAsync(headerSql, new { Id = stockTakeId }, tx);
                if (header is null) throw new KeyNotFoundException($"StockTake {stockTakeId} not found.");

                int status = (int)header.Status;
                if (status == (int)StockTakeStatus.Posted)
                    throw new InvalidOperationException("This stock take is already posted.");
                if (status != (int)StockTakeStatus.Approved)
                    throw new InvalidOperationException("Only Approved stock takes can be posted.");

                // 2) Lines
                var linesSql = @"
SELECT Id, StockTakeId, ItemId, BinId, SupplierId,OnHand, CountedQty, BadCountedQty, VarianceQty,
       Barcode, Remarks, ReasonId, Selected, IsActive
FROM StockTakeLines
WHERE StockTakeId = @Id AND IsActive = 1";
                if (onlySelected) linesSql += " AND Selected = 1;";
                else linesSql += ";";

                var lines = (await conn.QueryAsync<StockTakeLines>(linesSql, new { Id = stockTakeId }, tx)).ToList();

                // EARLY EXIT: no lines to process — still post header and ALL lines (new)
                if (lines.Count == 0)
                {
                    if (markPosted)
                    {
                        // post header
                        await conn.ExecuteAsync(
                            "UPDATE StockTake SET Status=@S, UpdatedBy=@U, UpdatedDate=SYSUTCDATETIME() WHERE Id=@Id;",
                            new { S = (int)StockTakeStatus.Posted, U = userName, Id = stockTakeId }, tx);

                        // post ALL lines of this take (selected/unselected)
                        await conn.ExecuteAsync(@"
UPDATE StockTakeLines
SET Status = @S, UpdatedBy = @U, UpdatedDate = SYSUTCDATETIME()
WHERE StockTakeId = @Id AND IsActive = 1;",
                            new { S = (int)StockTakeLineStatus.Posted, U = userName, Id = stockTakeId }, tx);
                    }

                    tx.Commit();
                    return 0;
                }

                var now = DateTime.UtcNow;
                var txnDate = txnDateOverride ?? now;

                // 3) Insert StockTakeInventoryAdjustment rows (variance-only) — unchanged logic
                const string insertAdjSql = @"
INSERT INTO StockTakeInventoryAdjustment
(
    ItemId, BinId, WarehouseTypeId, SupplierId,
    TxnDate, ReasonId, Remarks,
    SourceType, SourceId, SourceLineId,
    QtyIn, QtyOut,
    QtyBefore,CountedQty,BadCountedQty, QtyAfter,
    CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
VALUES
(
    @ItemId, @BinId, @WarehouseTypeId, @SupplierId,
    @TxnDate, @ReasonId, @Remarks,
    @SourceType, @SourceId, @SourceLineId,
    @QtyIn, @QtyOut,
    @QtyBefore,@CountedQty,@BadCountedQty, @QtyAfter,
    @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1
);";

                var adjRows = new List<object>();
                var postedLineIds = new List<int>(); // lines that produced an adjustment

                foreach (var l in lines)
                {
                    decimal before = l.OnHand;
                    decimal good = l.CountedQty ?? 0m;
                    decimal bad = l.BadCountedQty ?? 0m;
                    //decimal variance = l.VarianceQty ?? (good + bad - before);

                    //if (variance == 0m) continue;

                    decimal variance = l.VarianceQty ?? (good - before);   // GOOD ONLY
                    if (variance == 0m && bad == 0m)                       // skip only if truly no change
                        continue;

                    decimal qtyIn = variance > 0 ? variance : 0m;
                    decimal qtyOut = variance < 0 ? -variance : 0m;

                    adjRows.Add(new
                    {
                        ItemId = l.ItemId,
                        BinId = l.BinId,
                        WarehouseTypeId = (int)header.WarehouseTypeId,
                        SupplierId = l.SupplierId,
                        TxnDate = txnDate,
                        ReasonId = l.ReasonId,
                        Remarks = !string.IsNullOrWhiteSpace(l.Remarks) ? l.Remarks : remarks,
                        SourceType = "StockTake",
                        SourceId = stockTakeId,
                        SourceLineId = l.Id,
                        QtyIn = qtyIn,
                        QtyOut = qtyOut,
                        QtyBefore = before,
                        CountedQty = good,
                        BadCountedQty = bad,
                        QtyAfter = good,
                        CreatedBy = userName,
                        CreatedDate = now,
                        UpdatedBy = userName,
                        UpdatedDate = now
                    });

                    postedLineIds.Add(l.Id);
                }

                if (adjRows.Count == 0)
                {
                    // no variance lines — still post header & all lines (new)
                    if (markPosted)
                    {
                        await conn.ExecuteAsync(
                            "UPDATE StockTake SET Status=@S, UpdatedBy=@U, UpdatedDate=SYSUTCDATETIME() WHERE Id=@Id;",
                            new { S = (int)StockTakeStatus.Posted, U = userName, Id = stockTakeId }, tx);

                        await conn.ExecuteAsync(@"
UPDATE StockTakeLines
SET Status = @S, UpdatedBy = @U, UpdatedDate = SYSUTCDATETIME()
WHERE StockTakeId = @Id AND IsActive = 1;",
                            new { S = (int)StockTakeLineStatus.Posted, U = userName, Id = stockTakeId }, tx);
                    }

                    tx.Commit();
                    return 0;
                }

                await conn.ExecuteAsync(insertAdjSql, adjRows, tx); // writes to StockTakeInventoryAdjustment (unchanged)

                // 4) Apply to stock for lines that had variance (unchanged)
                //                if (applyToStock)
                //                {
                //                    const string upsertStockSql = @"
                //MERGE dbo.ItemWarehouseStock AS tgt
                //USING (VALUES (@ItemId, @WarehouseId, @BinId, @TotalPhysical))
                //      AS src (ItemId, WarehouseId, BinId, TotalPhysical)
                //ON (tgt.ItemId = src.ItemId
                //    AND tgt.WarehouseId = src.WarehouseId
                //    AND tgt.BinId = src.BinId)
                //WHEN MATCHED THEN
                //    UPDATE SET OnHand = src.TotalPhysical
                //WHEN NOT MATCHED THEN
                //    INSERT (ItemId, WarehouseId, BinId, OnHand, Reserved, MinQty, MaxQty, ReorderQty)
                //    VALUES (src.ItemId, src.WarehouseId, src.BinId, src.TotalPhysical, 0, 0, 0, 0);";

                //                    var upsertRows = lines
                //                        .Where(l => postedLineIds.Contains(l.Id))
                //                        .Select(l => new
                //                        {
                //                            ItemId = l.ItemId,
                //                            WarehouseId = (int)header.WarehouseTypeId,
                //                            BinId = l.BinId,
                //                            //TotalPhysical = (l.CountedQty ?? 0m) + (l.BadCountedQty ?? 0m)

                //                            // If there are any faulty pieces, keep OnHand = GOOD only.
                //                            // Otherwise (no faulty), good+bad == good anyway.
                //                            TotalPhysical = (l.BadCountedQty ?? 0m) > 0m
                //                                ? (l.CountedQty ?? 0m)                           // good only
                //                                : ((l.CountedQty ?? 0m) + (l.BadCountedQty ?? 0m)) // same as good when bad=0
                //                        })
                //                        .ToList();

                //                    await conn.ExecuteAsync(upsertStockSql, upsertRows, tx);
                //                }


                // 4) Apply delta to stock & supplier (variance lines only)
                if (applyToStock)
                {
                    // (A) Adjust warehouse/bin stock by DELTA, not overwrite
                    const string adjustWarehouseSql = @"
UPDATE S
SET S.OnHand = S.OnHand + @GoodDeltaWarehouse,
    S.Available = (ISNULL(S.OnHand,0) + @GoodDeltaWarehouse) - ISNULL(S.Reserved,0)
FROM dbo.ItemWarehouseStock S
WHERE S.ItemId      = @ItemId
  AND S.WarehouseId = @WarehouseId
  AND ( (S.BinId = @BinId) OR (S.BinId IS NULL AND @BinId IS NULL) );

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.ItemWarehouseStock
        (ItemId, WarehouseId, BinId, OnHand, Reserved, MinQty, MaxQty, ReorderQty)
    VALUES
        (@ItemId, @WarehouseId, @BinId, @GoodDeltaWarehouse, 0, 0, 0, 0);
END;
";


                    // (B) Adjust supplier-facing quantity (ItemPrice.Qty) by the same DELTA
                    // If multiple rows per supplier exist, update the most recent; otherwise insert.
                    const string adjustSupplierSql = @"
;WITH ip AS (
    SELECT TOP(1) *
    FROM dbo.ItemPrice
    WHERE ItemId = @ItemId 
      AND SupplierId = @SupplierId
      AND WarehouseId = @WarehouseId
    ORDER BY Id DESC
)
UPDATE ip 
SET Qty = ISNULL(Qty,0) + @GoodDeltaSupplier,
    BadCountedQty = ISNULL(BadCountedQty,0) + @BadDeltaSupplier;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.ItemPrice (ItemId, SupplierId, WarehouseId, Price, Barcode, Qty, BadCountedQty)
    VALUES (@ItemId, @SupplierId, @WarehouseId, 0, NULL, @GoodDeltaSupplier, @BadDeltaSupplier);
END;
";


                    // ✅ Load current supplier good+bad before posting (ItemPrice is source of truth)
                    var keyPairs = lines
                        .Where(l => postedLineIds.Contains(l.Id))
                        .Select(l => new { l.ItemId, l.SupplierId })
                        .Distinct()
                        .ToList();

                    var itemIds = keyPairs.Select(x => x.ItemId).Distinct().ToArray();
                    var supplierIds = keyPairs.Select(x => x.SupplierId).Distinct().ToArray();

                    const string ipBeforeSql = @"
SELECT
    ItemId,
    SupplierId,
    ISNULL(Qty,0)           AS GoodBefore,
    ISNULL(BadCountedQty,0) AS BadBefore
FROM dbo.ItemPrice
WHERE WarehouseId = @WarehouseId
  AND ItemId IN @ItemIds
  AND SupplierId IN @SupplierIds;
";

                    var ipBefore = (await conn.QueryAsync<(int ItemId, int SupplierId, decimal GoodBefore, decimal BadBefore)>(
                        ipBeforeSql,
                        new
                        {
                            WarehouseId = (int)header.WarehouseTypeId,
                            ItemIds = itemIds,
                            SupplierIds = supplierIds
                        },
                        tx
                    )).ToList();

                    // key = (ItemId, SupplierId)
                    var ipMap = ipBefore.ToDictionary(x => (x.ItemId, x.SupplierId), x => x);

                    // Build the per-line deltas from the already filtered variance-producing lines
                    var deltas = lines
       .Where(l => postedLineIds.Contains(l.Id))
       .Select(l =>
       {
           decimal good = l.CountedQty ?? 0m;
           decimal bad = l.BadCountedQty ?? 0m;

           // ✅ Warehouse stock "before" comes from StockTakeLines.OnHand
           decimal goodBeforeWarehouse = l.OnHand;

           // ✅ Supplier "before" must come from ItemPrice
           ipMap.TryGetValue(((int)l.ItemId, (int)l.SupplierId), out var before);
           decimal goodBeforeSupplier = before.GoodBefore;
           decimal badBeforeSupplier = before.BadBefore;

           // ✅ Warehouse delta = GOOD variance only
           decimal goodDeltaWarehouse = good - goodBeforeWarehouse;

           // ✅ Supplier delta should reflect counted vs ItemPrice
           decimal goodDeltaSupplier = good - goodBeforeSupplier;
           decimal badDeltaSupplier = bad - badBeforeSupplier;

           return new
           {
               ItemId = l.ItemId,
               WarehouseId = (int)header.WarehouseTypeId,
               BinId = l.BinId,
               SupplierId = l.SupplierId,

               // for warehouse update
               GoodDeltaWarehouse = goodDeltaWarehouse,

               // for supplier update
               GoodDeltaSupplier = goodDeltaSupplier,
               BadDeltaSupplier = badDeltaSupplier
           };
       })
       .ToList();




                    // Apply to warehouse/bin
                    foreach (var d in deltas)
                        await conn.ExecuteAsync(adjustWarehouseSql, d, tx);

                    // Apply to supplier qty
                    foreach (var d in deltas)
                        await conn.ExecuteAsync(adjustSupplierSql, d, tx);
                }

                // 5) Post the variance lines (existing)
                await conn.ExecuteAsync(@"
UPDATE StockTakeLines
SET Status = @S, UpdatedBy = @U, UpdatedDate = @D
WHERE Id IN @Ids;",
                    new
                    {
                        S = (int)StockTakeLineStatus.Posted,
                        U = userName,
                        D = now,
                        Ids = postedLineIds
                    }, tx);

                // 6) Ensure ALL lines are Posted when the take is posted (new)
                if (markPosted)
                {
                    await conn.ExecuteAsync(@"
UPDATE StockTakeLines
SET Status = @S, UpdatedBy = @U, UpdatedDate = @D
WHERE StockTakeId = @StockTakeId AND IsActive = 1;",
                        new
                        {
                            S = (int)StockTakeLineStatus.Posted,
                            U = userName,
                            D = now,
                            StockTakeId = stockTakeId
                        }, tx);
                }

                // 7) Post header (existing)
                if (markPosted)
                {
                    await conn.ExecuteAsync(
                        "UPDATE StockTake SET Status=@S, UpdatedBy=@U, UpdatedDate=@D WHERE Id=@Id;",
                        new { S = (int)StockTakeStatus.Posted, U = userName, D = now, Id = stockTakeId }, tx);
                }

                tx.Commit();
                return postedLineIds.Count; // count of lines that produced adjustments
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }



    }
}
