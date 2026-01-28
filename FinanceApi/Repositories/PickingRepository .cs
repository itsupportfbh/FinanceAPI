// Repositories/PickingRepository.cs
using Dapper;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;
using Microsoft.Data.SqlClient;
using FinanceApi.Data;

namespace FinanceApi.Repositories
{
    public class PickingRepository : DynamicRepository, IPickingRepository
    {
        public PickingRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory) { }

        public async Task<IEnumerable<PickingDTO>> GetAllAsync()
        {
            const string hdrSql = @"
SELECT
    p.Id, p.SoId, p.SoDate,p.DeliveryDate,c.CustomerName,s.salesOrderNo,
    p.BarCode, p.QrCode, p.BarCodeSrc,p.QrCodeSrc,
    p.Status, p.CreatedBy, p.CreatedDate, p.UpdatedBy, p.UpdatedDate,p.IsActive
FROM Picking p 
LEFT JOIN dbo.salesorder s ON s.Id = p.SoId
LEFT JOIN dbo.Customer c ON c.Id = s.CustomerId
WHERE p.IsActive = 1
ORDER BY p.Id DESC;";

            var headers = (await Connection.QueryAsync<PickingDTO>(hdrSql)).ToList();
            if (headers.Count == 0) return headers;

            // collect header ids
            var ids = headers.Select(h => h.Id).ToArray();
            if (ids.Length == 0) return headers; // guard against empty IN ()

            const string lineSql = @"
SELECT
    pl.Id, pl.PickId, pl.SoLineId, pl.ItemId, i.ItemName,
    ISNULL(w.Name,'')  AS WarehouseName,
    ISNULL(s.Name,'')  AS SupplierName,
    ISNULL(b.BinName,'') AS Bin,
    pl.WarehouseId, pl.SupplierId, pl.BinId,
    pl.Quantity, pl.CartonId,
    pl.CreatedBy, pl.CreatedDate, pl.UpdatedBy, pl.UpdatedDate, pl.IsActive
FROM dbo.PickingLine pl
LEFT JOIN dbo.Item      i ON i.Id = pl.ItemId
LEFT JOIN dbo.Warehouse w ON w.Id = pl.WarehouseId
LEFT JOIN dbo.Suppliers s ON s.Id = pl.SupplierId
LEFT JOIN dbo.Bin       b ON b.Id = pl.BinId
WHERE pl.PickId IN @Ids AND pl.IsActive = 1   -- Dapper expands @Ids
ORDER BY pl.Id;";

            // ✅ param name MUST match @Ids for Dapper list expansion
            var lines = (await Connection.QueryAsync<PickingLineDTO>(lineSql, new { Ids = ids })).ToList();

            var byId = headers.ToDictionary(h => h.Id);
            foreach (var ln in lines)
            {
                if (byId.TryGetValue(ln.PickId, out var parent))
                    parent.LineItems.Add(ln);
            }

            return headers;

        }

        public async Task<PickingDTO?> GetByIdAsync(int id)
        {
            const string hdrSql = @"
SELECT TOP (1)
    Id, SoId, SoDate,DeliveryDate,
    BarCode, QrCode, BarCodeSrc, QrCodeSrc,
    Status, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
FROM Picking
WHERE Id = @Id AND IsActive = 1;";

            var header = await Connection.QueryFirstOrDefaultAsync<PickingDTO>(hdrSql, new { Id = id });
            if (header == null) return null;

            const string lineSql = @"
SELECT
    pl.Id, pl.PickId, pl.SoLineId, pl.ItemId,i.ItemName,ISNULL(w.Name, '') AS WarehouseName, 
    ISNULL(s.Name, '') AS SupplierName,ISNULL(b.BinName, '') AS Bin,
    pl.WarehouseId, pl.SupplierId, pl.BinId,
    pl.Quantity, pl.CartonId,
    pl.CreatedBy, pl.CreatedDate, pl.UpdatedBy, pl.UpdatedDate, pl.IsActive
FROM PickingLine pl
LEFT JOIN dbo.Item      i ON i.Id = pl.ItemId
LEFT JOIN dbo.Warehouse      w ON w.Id = pl.WarehouseId
LEFT JOIN dbo.Suppliers       s ON s.Id = pl.SupplierId
LEFT JOIN dbo.Bin            b ON b.Id = pl.BinId
WHERE pl.PickId = @Id AND pl.IsActive = 1
ORDER BY pl.Id;";

            var lines = await Connection.QueryAsync<PickingLineDTO>(lineSql, new { Id = id });
            header.LineItems = lines.ToList();

            return header;
        }

        public async Task<int> CreateAsync(Picking p)
        {
            if (p is null) throw new ArgumentNullException(nameof(p));

            var now = DateTime.UtcNow;
            if (p.CreatedDate == default) p.CreatedDate = now;
            if (p.UpdatedDate == null) p.UpdatedDate = now;

            const string insertHdr = @"
INSERT INTO Picking
(
    SoId, SoDate,DeliveryDate,
    BarCode, QrCode, BarCodeSrc, QrCodeSrc,
    Status,
    CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
OUTPUT INSERTED.Id
VALUES
(
    @SoId, @SoDate,@DeliveryDate,
    @BarCode, @QrCode, @BarCodeSrc, @QrCodeSrc,
    @Status,
    @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1
);";

            const string insertLine = @"
INSERT INTO PickingLine
(
    PickId, SoLineId, ItemId,
    WarehouseId, SupplierId, BinId,
    Quantity, CartonId,
    CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
VALUES
(
    @PickId, @SoLineId, @ItemId,
    @WarehouseId, @SupplierId, @BinId,
    @Quantity, @CartonId,
    @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1
);";

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {
                var newId = await conn.ExecuteScalarAsync<int>(insertHdr, new
                {
                    p.SoId,
                    p.SoDate,
                    p.DeliveryDate,
                    p.BarCode,
                    p.QrCode,
                    p.BarCodeSrc,
                    p.QrCodeSrc,
                    p.Status,
                    p.CreatedBy,
                    p.CreatedDate,
                    p.UpdatedBy,
                    UpdatedDate = p.UpdatedDate ?? now
                }, tx);

                if (p.LineItems?.Count > 0)
                {
                    var lp = p.LineItems.Select(l => new
                    {
                        PickId = newId,
                        l.SoLineId,
                        l.ItemId,
                        l.WarehouseId,
                        l.SupplierId,
                        l.BinId,
                        l.Quantity,
                        l.CartonId,
                        CreatedBy = p.CreatedBy,
                        CreatedDate = p.CreatedDate,
                        UpdatedBy = p.UpdatedBy,
                        UpdatedDate = p.UpdatedDate ?? now
                    });

                    await conn.ExecuteAsync(insertLine, lp, tx);
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

        public async Task UpdateAsync(Picking p)
        {
            if (p is null) throw new ArgumentNullException(nameof(p));
            var now = DateTime.UtcNow;

            const string updHdr = @"
UPDATE Picking
SET
    SoId       = @SoId,
    SoDate     = @SoDate,
    DeliveryDate = @DeliveryDate,
    BarCode    = @BarCode,
    QrCode     = @QrCode,
    BarCodeSrc = @BarCodeSrc,
    QrCodeSrc  = @QrCodeSrc,
    Status     = @Status,
    UpdatedBy  = @UpdatedBy,
    UpdatedDate  = @UpdatedDate
WHERE Id = @Id AND IsActive = 1;";

            const string updLine = @"
UPDATE PickingLine
SET
    SoLineId   = @SoLineId,
    ItemId     = @ItemId,
    WarehouseId= @WarehouseId,
    SupplierId = @SupplierId,
    BinId      = @BinId,
    Quantity = @Quantity,
    CartonId   = @CartonId,
    UpdatedBy  = @UpdatedBy,
    UpdatedDate  = @UpdatedDate,
    IsActive   = 1
WHERE Id = @Id AND PickId = @PickId;";

            const string insLine = @"
INSERT INTO PickingLine
(
    PickId, SoLineId, ItemId,
    WarehouseId, SupplierId, BinId,
    Quantity, CartonId,
    CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
OUTPUT INSERTED.Id
VALUES
(
    @PickId, @SoLineId, @ItemId,
    @WarehouseId, @SupplierId, @BinId,
    @Quantity, @CartonId,
    @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1
);";

            const string softDeleteMissing = @"
UPDATE PickingLine
SET IsActive = 0, UpdatedBy = @UpdatedBy, UpdatedDate = @UpdatedDate
WHERE PickId = @PickId
  AND IsActive = 1
  AND (@KeepIdsCount = 0 OR Id NOT IN @KeepIds);";

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {
                await conn.ExecuteAsync(updHdr, new
                {
                    p.SoId,
                    p.SoDate,
                    p.DeliveryDate,
                    p.BarCode,
                    p.QrCode,
                    p.BarCodeSrc,
                    p.QrCodeSrc,
                    p.Status,
                    p.UpdatedBy,
                    UpdatedDate = now,
                    p.Id
                }, tx);

                var keepIds = new List<int>();
                if (p.LineItems?.Count > 0)
                {
                    foreach (var l in p.LineItems)
                    {
                        if (l.Id > 0)
                        {
                            await conn.ExecuteAsync(updLine, new
                            {
                                l.Id,
                                PickId = p.Id,
                                l.SoLineId,
                                l.ItemId,
                                l.WarehouseId,
                                l.SupplierId,
                                l.BinId,
                                l.Quantity,
                                l.CartonId,
                                p.UpdatedBy,
                                UpdatedDate = now
                            }, tx);
                            keepIds.Add(l.Id);
                        }
                        else
                        {
                            var newLineId = await conn.ExecuteScalarAsync<int>(insLine, new
                            {
                                PickId = p.Id,
                                l.SoLineId,
                                l.ItemId,
                                l.WarehouseId,
                                l.SupplierId,
                                l.BinId,
                                l.Quantity,
                                l.CartonId,
                                CreatedBy = p.UpdatedBy ?? p.CreatedBy,
                                CreatedDate = now,
                                p.UpdatedBy,
                                UpdatedDate = now
                            }, tx);
                            keepIds.Add(newLineId);
                        }
                    }
                }

                var keepIdsParam = keepIds.Count == 0 ? new[] { -1 } : keepIds.ToArray();

                await conn.ExecuteAsync(softDeleteMissing, new
                {
                    PickId = p.Id,
                    KeepIds = keepIdsParam,
                    KeepIdsCount = keepIds.Count,
                    p.UpdatedBy,
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
UPDATE Picking
SET IsActive = 0, UpdatedBy = @UpdatedBy, UpdatedDate = SYSUTCDATETIME()
WHERE Id = @Id AND IsActive = 1;";

            const string sqlLines = @"
UPDATE PickingLine
SET IsActive = 0, UpdatedBy = @UpdatedBy, UpdatedDate = SYSUTCDATETIME()
WHERE PickId = @Id AND IsActive = 1;";

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {
                var affected = await conn.ExecuteAsync(sqlHeader, new { Id = id, UpdatedBy = updatedBy }, tx);
                if (affected == 0)
                    throw new KeyNotFoundException("Picking not found.");

                await conn.ExecuteAsync(sqlLines, new { Id = id, UpdatedBy = updatedBy }, tx);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }
    }
}
