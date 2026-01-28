using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class MaterialRequisitionRepository : DynamicRepository, IMaterialRequisitionRepository
    {
        public MaterialRequisitionRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        // ✅ GetAll with Lines (includes BinId + BinName)
        public async Task<IEnumerable<MaterialRequisitionDTO>> GetAllAsync()
        {
            const string sql = @"
SELECT
    mr.Id,
    mr.ReqNo,
    mr.OutletId,
    mr.BinId,                         -- ✅ NEW
    mr.RequesterName,
    mr.ReqDate,
    mr.Status,
    mr.CreatedBy,
    mr.CreatedDate,
    mr.UpdatedBy,
    mr.UpdatedDate,
    mr.IsActive,
    wh.Name AS OutletName,
    b.BinName AS BinName              -- ✅ OPTIONAL (change table if needed)
FROM dbo.MaterialRequisition mr
INNER JOIN dbo.Warehouse wh ON wh.Id = mr.OutletId
LEFT JOIN dbo.Bin b ON b.Id = mr.BinId   -- ✅ CHANGE BinMaster if your table name differs
WHERE mr.IsActive = 1
ORDER BY mr.Id DESC;

SELECT
    l.Id,
    l.MaterialReqId,
    l.ItemId,
    l.ItemCode,
    l.ItemName,
    l.UomId,
    l.UomName,
    l.Qty,
l.ReceivedQty,
    l.CreatedDate
FROM dbo.MaterialRequisitionLine l
INNER JOIN dbo.MaterialRequisition mr ON mr.Id = l.MaterialReqId
WHERE mr.IsActive = 1;";

            using var multi = await Connection.QueryMultipleAsync(sql);

            var headers = (await multi.ReadAsync<MaterialRequisitionDTO>()).ToList();
            var lines = (await multi.ReadAsync<MaterialRequisitionLineDTO>()).ToList();

            // group lines by header id
            var lineLookup = lines
                .GroupBy(x => x.MaterialReqId)
                .ToDictionary(g => g.Key, g => (ICollection<MaterialRequisitionLineDTO>)g.ToList());

            foreach (var h in headers)
            {
                h.Lines = lineLookup.TryGetValue(h.Id, out var lns)
                    ? lns
                    : new List<MaterialRequisitionLineDTO>();
            }

            return headers;
        }

        // ✅ GetById with DTO Lines (includes BinId + BinName)
        public async Task<MaterialRequisitionDTO?> GetByIdAsync(int id)
        {
            const string sql = @"
SELECT
    mr.Id,
    mr.ReqNo,
    mr.OutletId,
    mr.BinId,                         -- ✅ NEW
    mr.RequesterName,
    mr.ReqDate,
    mr.Status,
    mr.CreatedBy,
    mr.CreatedDate,
    mr.UpdatedBy,
    mr.UpdatedDate,
    mr.IsActive,
    wh.Name AS OutletName,
    b.BinName AS BinName              -- ✅ OPTIONAL
FROM dbo.MaterialRequisition mr
INNER JOIN dbo.Warehouse wh ON wh.Id = mr.OutletId
LEFT JOIN dbo.Bin b ON b.Id = mr.BinId   -- ✅ CHANGE BinMaster if your table name differs
WHERE mr.Id = @Id AND mr.IsActive = 1;

SELECT
    l.Id,
    l.MaterialReqId,
    l.ItemId,
    l.ItemCode,
    l.ItemName,
    l.UomId,
    l.UomName,
    l.Qty,
l.ReceivedQty,
    l.CreatedDate
FROM dbo.MaterialRequisitionLine l
WHERE l.MaterialReqId = @Id
ORDER BY l.Id;";

            using var multi = await Connection.QueryMultipleAsync(sql, new { Id = id });

            var header = await multi.ReadSingleOrDefaultAsync<MaterialRequisitionDTO>();
            if (header == null) return null;

            var lines = (await multi.ReadAsync<MaterialRequisitionLineDTO>()).ToList();
            header.Lines = lines;

            return header;
        }

        // ✅ Create (includes BinId)
        public async Task<int> CreateAsync(MaterialRequisition mrq)
        {
            const string getNextReqNoSql = @"
DECLARE @Prefix NVARCHAR(20) = 'MRQ-' + CONVERT(VARCHAR(8), GETDATE(), 112) + '-';

DECLARE @NextSeq INT =
(
  SELECT ISNULL(MAX(CAST(RIGHT(ReqNo, 4) AS INT)), 0) + 1
  FROM dbo.MaterialRequisition WITH (UPDLOCK, HOLDLOCK)
  WHERE ReqNo LIKE @Prefix + '%'
);

SELECT @Prefix + RIGHT('0000' + CAST(@NextSeq AS VARCHAR(4)), 4);
";

            const string insertHeaderSql = @"
INSERT INTO dbo.MaterialRequisition
    (ReqNo, OutletId, BinId, RequesterName, ReqDate, Status,
     CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
OUTPUT INSERTED.Id
VALUES
    (@ReqNo, @OutletId, @BinId, @RequesterName, @ReqDate, @Status,
     @CreatedBy, COALESCE(@CreatedDate, SYSUTCDATETIME()), @UpdatedBy, @UpdatedDate, @IsActive);";

            const string insertLineSql = @"
INSERT INTO dbo.MaterialRequisitionLine
    (MaterialReqId, ItemId, ItemCode, ItemName, UomId, UomName, Qty, CreatedDate)
VALUES
    (@MaterialReqId, @ItemId, @ItemCode, @ItemName, @UomId, @UomName, @Qty,
     COALESCE(@CreatedDate, SYSUTCDATETIME()));";

            using var conn = Connection;
            conn.Open();

            using var tx = conn.BeginTransaction();

            try
            {
                // ✅ generate reqno inside transaction
                var reqNo = await conn.QuerySingleAsync<string>(getNextReqNoSql, transaction: tx);

                // ✅ header insert
                var headerId = await conn.QuerySingleAsync<int>(insertHeaderSql, new
                {
                    ReqNo = reqNo,
                    mrq.OutletId,
                    mrq.BinId,             // ✅ NEW
                    mrq.RequesterName,
                    mrq.ReqDate,
                    mrq.Status,
                    mrq.CreatedBy,
                    mrq.CreatedDate,
                    mrq.UpdatedBy,
                    mrq.UpdatedDate,
                    mrq.IsActive
                }, tx);

                // ✅ lines insert
                if (mrq.Lines != null && mrq.Lines.Count > 0)
                {
                    foreach (var line in mrq.Lines)
                    {
                        await conn.ExecuteAsync(insertLineSql, new
                        {
                            MaterialReqId = headerId,
                            line.ItemId,
                            line.ItemCode,
                            line.ItemName,
                            line.UomId,
                            line.UomName,
                            line.Qty,
                            line.CreatedDate
                        }, tx);
                    }
                }

                tx.Commit();
                return headerId;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ✅ Update (includes BinId)
        public async Task UpdateAsync(MaterialRequisition mrq)
        {
            const string updateHeaderSql = @"
UPDATE dbo.MaterialRequisition
SET
    OutletId      = @OutletId,
    BinId         = @BinId,              -- ✅ NEW
    RequesterName = @RequesterName,
    ReqDate       = @ReqDate,
    Status        = @Status,
    UpdatedBy     = @UpdatedBy,
    UpdatedDate   = COALESCE(@UpdatedDate, SYSUTCDATETIME())
WHERE Id = @Id;";

            // 1) Update existing line by Line.Id
            const string updateLineSql = @"
UPDATE dbo.MaterialRequisitionLine
SET
    ItemId    = @ItemId,
    ItemCode  = @ItemCode,
    ItemName  = @ItemName,
    UomId     = @UomId,
    UomName   = @UomName,
    Qty       = @Qty
WHERE Id = @Id
  AND MaterialReqId = @MaterialReqId;";

            // 2) Insert new line (Id = 0)
            const string insertLineSql = @"
INSERT INTO dbo.MaterialRequisitionLine
    (MaterialReqId, ItemId, ItemCode, ItemName, UomId, UomName, Qty, CreatedDate)
VALUES
    (@MaterialReqId, @ItemId, @ItemCode, @ItemName, @UomId, @UomName, @Qty,
     COALESCE(@CreatedDate, SYSUTCDATETIME()));";

            // 3) Delete removed lines (those not in payload)
            const string deleteRemovedLinesSql = @"
DELETE FROM dbo.MaterialRequisitionLine
WHERE MaterialReqId = @MaterialReqId
  AND (@KeepIdsCount = 0 OR Id NOT IN @KeepIds);";

            using var conn = Connection;
            conn.Open();
            using var tx = conn.BeginTransaction();

            try
            {
                // header update
                await conn.ExecuteAsync(updateHeaderSql, new
                {
                    mrq.Id,
                    mrq.OutletId,
                    mrq.BinId,             // ✅ NEW
                    mrq.RequesterName,
                    mrq.ReqDate,
                    mrq.Status,
                    mrq.UpdatedBy,
                    mrq.UpdatedDate
                }, tx);

                var lines = mrq.Lines ?? new List<MaterialRequisitionLine>();

                // KeepIds = existing line ids coming from UI
                var keepIds = lines.Where(x => x.Id > 0).Select(x => x.Id).Distinct().ToList();

                // delete removed lines
                await conn.ExecuteAsync(deleteRemovedLinesSql, new
                {
                    MaterialReqId = mrq.Id,
                    KeepIds = keepIds,
                    KeepIdsCount = keepIds.Count
                }, tx);

                // update/insert
                foreach (var line in lines)
                {
                    if (line.Id > 0)
                    {
                        // update existing row
                        await conn.ExecuteAsync(updateLineSql, new
                        {
                            Id = line.Id,
                            MaterialReqId = mrq.Id,
                            line.ItemId,
                            line.ItemCode,
                            line.ItemName,
                            line.UomId,
                            line.UomName,
                            line.Qty
                        }, tx);
                    }
                    else
                    {
                        // insert new row
                        await conn.ExecuteAsync(insertLineSql, new
                        {
                            MaterialReqId = mrq.Id,
                            line.ItemId,
                            line.ItemCode,
                            line.ItemName,
                            line.UomId,
                            line.UomName,
                            line.Qty,
                            line.CreatedDate
                        }, tx);
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

        public async Task DeactivateAsync(int id)
        {
            const string sql = @"UPDATE dbo.MaterialRequisition SET IsActive = 0 WHERE Id = @Id;";
            await Connection.ExecuteAsync(sql, new { Id = id });
        }
    }
}
