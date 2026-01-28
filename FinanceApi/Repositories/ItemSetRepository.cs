using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;

namespace FinanceApi.Repositories
{
    public class ItemSetRepository : DynamicRepository, IItemSetRepository
    {
        public ItemSetRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        // =========================
        // GET ALL (with Items)
        // =========================
        public async Task<IEnumerable<ItemSetDTO>> GetAllAsync()
        {
            const string sql = @"
SELECT
    s.Id, s.SetName, s.CreatedBy, s.UpdatedBy, s.CreatedDate, s.UpdatedDate, s.IsActive,
    i.Id, i.ItemSetId, i.ItemId, i.CreatedBy, i.CreatedDate, i.IsActive
FROM dbo.ItemSet s
LEFT JOIN dbo.ItemSetItem i
       ON i.ItemSetId = s.Id AND i.IsActive = 1
ORDER BY s.Id DESC, i.Id;";

            var lookup = new Dictionary<long, ItemSetDTO>();

            await Connection.QueryAsync<ItemSetDTO, ItemSetItemDTO, ItemSetDTO>(
                sql,
                (set, item) =>
                {
                    if (!lookup.TryGetValue(set.Id, out var existing))
                    {
                        existing = set;
                        existing.Items = new List<ItemSetItemDTO>();
                        lookup.Add(existing.Id, existing);
                    }

                    if (item != null && item.Id > 0)
                        existing.Items.Add(item);

                    return existing;
                },
                splitOn: "Id"
            );

            return lookup.Values;
        }

        // =========================
        // GET BY ID (with Items)
        // =========================
        public async Task<ItemSetDTO> GetByIdAsync(int id)
        {
            const string sql = @"
SELECT
    s.Id, s.SetName, s.CreatedBy, s.UpdatedBy, s.CreatedDate, s.UpdatedDate, s.IsActive,
    i.Id, i.ItemSetId, i.ItemId, i.CreatedBy, i.CreatedDate, i.IsActive,u.Name as UomName
FROM dbo.ItemSet s
LEFT JOIN dbo.ItemSetItem i
       ON i.ItemSetId = s.Id AND i.IsActive = 1
LEFT JOIN dbo.Item  it  on it.Id = i.ItemId 
Left Join dbo.Uom  u on u.Id = it.UomId
WHERE s.Id = @Id
ORDER BY i.Id;";

            var lookup = new Dictionary<long, ItemSetDTO>();

            await Connection.QueryAsync<ItemSetDTO, ItemSetItemDTO, ItemSetDTO>(
                sql,
                (set, item) =>
                {
                    if (!lookup.TryGetValue(set.Id, out var existing))
                    {
                        existing = set;
                        existing.Items = new List<ItemSetItemDTO>();
                        lookup.Add(existing.Id, existing);
                    }

                    if (item != null && item.Id > 0)
                        existing.Items.Add(item);

                    return existing;
                },
                new { Id = id },
                splitOn: "Id"
            );

            // if not found -> return null? (but your signature returns ItemSetDTO)
            // so throw like QuerySingle used to do
            if (!lookup.Any())
                throw new KeyNotFoundException($"ItemSet not found. Id={id}");

            return lookup.Values.First();
        }

        // =========================
        // CREATE (Header + Items)
        // IMPORTANT: itemSet.Items must have ItemId list
        // =========================
        public async Task<int> CreateAsync(ItemSet itemSet)
        {
            if (string.IsNullOrWhiteSpace(itemSet.SetName))
                throw new ArgumentException("SetName is required.");

            var itemIds = itemSet.Items?
                .Where(x => x.IsActive)
                .Select(x => x.ItemId)
                .Distinct()
                .ToList() ?? new List<long>();

            // ✅ IMPORTANT: take Connection ONCE
            using var conn = Connection;
            if (conn.State != ConnectionState.Open)
                conn.Open();

            using var tx = conn.BeginTransaction();
            try
            {
                const string dupSql =
                    @"SELECT COUNT(1) FROM dbo.ItemSet WHERE IsActive = 1 AND SetName = @SetName;";

                var exists = await conn.ExecuteScalarAsync<int>(
                    dupSql,
                    new { SetName = itemSet.SetName.Trim() },
                    tx
                );

                if (exists > 0) throw new InvalidOperationException("SetName already exists.");

                const string insertHeader = @"
INSERT INTO dbo.ItemSet (SetName, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
OUTPUT INSERTED.Id
VALUES (@SetName, @CreatedBy, SYSUTCDATETIME(), NULL, NULL, @IsActive);";

                var newId = await conn.ExecuteScalarAsync<int>(
                    insertHeader,
                    new
                    {
                        SetName = itemSet.SetName.Trim(),
                        CreatedBy = itemSet.CreatedBy ?? "system",
                        IsActive = itemSet.IsActive ? 1 : 0
                    },
                    tx
                );

                if (itemIds.Count > 0)
                {
                    const string insertItem = @"
INSERT INTO dbo.ItemSetItem (ItemSetId, ItemId, CreatedBy, CreatedDate, IsActive)
VALUES (@ItemSetId, @ItemId, @CreatedBy, SYSUTCDATETIME(), 1);";

                    foreach (var itemId in itemIds)
                    {
                        await conn.ExecuteAsync(
                            insertItem,
                            new
                            {
                                ItemSetId = newId,
                                ItemId = itemId,
                                CreatedBy = itemSet.CreatedBy ?? "system"
                            },
                            tx
                        );
                    }
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


        // =========================
        // UPDATE (Header + Items)
        // IMPORTANT: itemSet.Items must have ItemId list
        // =========================
        public async Task UpdateAsync(ItemSet itemSet)
        {
            if (itemSet.Id <= 0) throw new ArgumentException("Id is required.");
            if (string.IsNullOrWhiteSpace(itemSet.SetName)) throw new ArgumentException("SetName is required.");

            var desired = itemSet.Items?
                .Where(x => x.IsActive)
                .Select(x => x.ItemId)
                .Distinct()
                .ToList() ?? new List<long>();

            // ✅ IMPORTANT: take Connection ONCE
            using var conn = Connection;
            if (conn.State != ConnectionState.Open)
                conn.Open();

            using var tx = conn.BeginTransaction();
            try
            {
                const string existsSql = @"SELECT COUNT(1) FROM dbo.ItemSet WHERE Id = @Id;";
                var ok = await conn.ExecuteScalarAsync<int>(existsSql, new { itemSet.Id }, tx);
                if (ok == 0) throw new KeyNotFoundException("ItemSet not found.");

                const string dupSql = @"
SELECT COUNT(1)
FROM dbo.ItemSet
WHERE IsActive = 1 AND SetName = @SetName AND Id <> @Id;";
                var dup = await conn.ExecuteScalarAsync<int>(
                    dupSql,
                    new { SetName = itemSet.SetName.Trim(), itemSet.Id },
                    tx
                );
                if (dup > 0) throw new InvalidOperationException("SetName already exists.");

                const string updateHeader = @"
UPDATE dbo.ItemSet
SET SetName = @SetName,
    UpdatedBy = @UpdatedBy,
    UpdatedDate = SYSUTCDATETIME(),
    IsActive = @IsActive
WHERE Id = @Id;";

                await conn.ExecuteAsync(
                    updateHeader,
                    new
                    {
                        Id = itemSet.Id,
                        SetName = itemSet.SetName.Trim(),
                        UpdatedBy = itemSet.UpdatedBy ?? "system",
                        IsActive = itemSet.IsActive ? 1 : 0
                    },
                    tx
                );

                const string deactivateAll = @"UPDATE dbo.ItemSetItem SET IsActive = 0 WHERE ItemSetId = @Id;";
                await conn.ExecuteAsync(deactivateAll, new { Id = itemSet.Id }, tx);

                if (desired.Count > 0)
                {
                    const string reactivate = @"
UPDATE dbo.ItemSetItem
SET IsActive = 1
WHERE ItemSetId = @ItemSetId AND ItemId = @ItemId;";

                    const string insertIfMissing = @"
IF NOT EXISTS (SELECT 1 FROM dbo.ItemSetItem WHERE ItemSetId = @ItemSetId AND ItemId = @ItemId)
BEGIN
  INSERT INTO dbo.ItemSetItem (ItemSetId, ItemId, CreatedBy, CreatedDate, IsActive)
  VALUES (@ItemSetId, @ItemId, @CreatedBy, SYSUTCDATETIME(), 1);
END";

                    foreach (var itemId in desired)
                    {
                        await conn.ExecuteAsync(reactivate, new { ItemSetId = itemSet.Id, ItemId = itemId }, tx);

                        await conn.ExecuteAsync(
                            insertIfMissing,
                            new
                            {
                                ItemSetId = itemSet.Id,
                                ItemId = itemId,
                                CreatedBy = itemSet.UpdatedBy ?? "system"
                            },
                            tx
                        );
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


        // =========================
        // DEACTIVATE (soft delete)
        // =========================
        public async Task DeactivateAsync(int id)
        {
            const string query = @"
UPDATE dbo.ItemSet
SET IsActive = 0,
    UpdatedBy = ISNULL(UpdatedBy,'system'),
    UpdatedDate = SYSUTCDATETIME()
WHERE Id = @Id;";

            await Connection.ExecuteAsync(query, new { Id = id });
        }
    }
}
