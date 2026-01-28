using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;


namespace FinanceApi.Repositories
{
    public class WarehouseRepository : DynamicRepository,IWarehouseRepository
    {
        private readonly ApplicationDbContext _context;

        public WarehouseRepository(IDbConnectionFactory connectionFactory)
        : base(connectionFactory)
        {

        }

        public async Task<IEnumerable<WarehouseDto>> GetAllAsync()
        {
            var query = @"
                        SELECT 
                            w.Id,
                            w.Name,
                            w.CityId,
                            ISNULL(c.CityName, '') AS CityName,
                            w.StateId,
                            ISNULL(s.StateName, '') AS StateName,
                            w.CountryId,
                            ISNULL(co.CountryName, '') AS CountryName,
                            w.BinID,
                            w.Address,
                            w.Description,
                            w.Phone,
                            w.CreatedBy,
                            w.CreatedDate,
                            w.UpdatedBy,
                            w.UpdatedDate,
                            w.IsActive
                        FROM Warehouse w
                        LEFT JOIN City c ON w.CityId = c.Id
                        LEFT JOIN State s ON w.StateId = s.Id
                        LEFT JOIN Country co ON w.CountryId = co.Id
                        WHERE w.IsActive = 1
                        ORDER BY w.Id";

            var rows = await Connection.QueryAsync<WarehouseDto>(query);
            return rows.ToList();
        }

        public async Task<WarehouseDto?> GetByIdAsync(int id)
        {
            const string sql = @"
                            SELECT 
                                w.Id,
                                w.Name,
                                w.CityId,
                                ISNULL(c.CityName, '') AS CityName,
                                w.StateId,
                                ISNULL(s.StateName, '') AS StateName,
                                w.CountryId,
                                ISNULL(co.CountryName, '') AS CountryName,
                                w.Address,
                                w.Description,
                                w.Phone,
                                w.CreatedBy,
                                w.CreatedDate,
                                w.UpdatedBy,
                                w.UpdatedDate,
                                w.IsActive
                            FROM Warehouse w
                            LEFT JOIN City c ON w.CityId = c.Id
                            LEFT JOIN State s ON w.StateId = s.Id
                            LEFT JOIN Country co ON w.CountryId = co.Id
                            WHERE w.Id = @id AND w.IsActive = 1;";

            var result = await Connection.QueryFirstOrDefaultAsync<WarehouseDto>(sql, new { id });
            return result;

        }

        public async Task<int> CreateAsync(Warehouse warehouse)
        {
            const string query = @"INSERT INTO Warehouse (Name,BinID,CountryId,StateId,CityId,Phone,Address,Description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@Name,@BinID,@CountryId,@StateId,@CityId,@Phone,@Address,@Description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, warehouse);
        }

        public async Task UpdateAsync(Warehouse updatedWarehouse)
        {
            const string query = "UPDATE Warehouse SET Name = @Name,BinID = @BinID,CountryId = @CountryId,StateId = @StateId,CityId = @CityId,Phone = @Phone,Address = @Address,Description = @Description WHERE Id = @Id";
            await Connection.ExecuteAsync(query, updatedWarehouse);
        }


        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Warehouse SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
        public async Task<IEnumerable<WarehouseDto>> GetBinNameByIdAsync(int id)
        {
            var query = @"
                        SELECT
    w.Id   AS WarehouseId,
    w.Name AS WarehouseName,
    b.ID   AS BinID,
    b.BinName
FROM Warehouse AS w
CROSS APPLY STRING_SPLIT(w.BinId, ',') AS s
CROSS APPLY (SELECT TRY_CONVERT(bigint, LTRIM(RTRIM(s.value))) AS BinIdVal) AS x
INNER JOIN BIN AS b
    ON b.ID = x.BinIdVal
WHERE
    x.BinIdVal IS NOT NULL
    AND b.IsActive = 1
    AND w.Id = @Id;";

            var rows = await Connection.QueryAsync<WarehouseDto>(query, new { Id = id });
            return rows.ToList();
        }



        public async Task<IEnumerable<WarehouseDto>> GetNameByWarehouseAsync(string name)
        {
            const string sql = @"
        SELECT *
        FROM Warehouse
        WHERE Name <> @name
          AND IsActive = 1;
    ";

            var result = await Connection.QueryAsync<WarehouseDto>(sql, new { name });
            return result;
        }


    }
}
