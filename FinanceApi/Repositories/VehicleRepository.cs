using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;

namespace FinanceApi.Repositories
{
    public class VehicleRepository : DynamicRepository, IVehicleRepository
    {
        
        public VehicleRepository(IDbConnectionFactory connectionFactory)
             : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<VehicleDTO>> GetAllAsync(bool onlyActive = true)
        {
            var sql = @"
SELECT Id, VehicleNo, VehicleType, Capacity, CapacityUom, IsActive
FROM dbo.Vehicle
WHERE (@OnlyActive = 0 OR IsActive = 1)
ORDER BY Id";
            return await Connection.QueryAsync<VehicleDTO>(sql, new { OnlyActive = onlyActive ? 1 : 0 });
        }

        public async Task<VehicleDTO?> GetByIdAsync(int id)
        {
            const string sql = @"
SELECT Id, VehicleNo, VehicleType, Capacity, CapacityUom, IsActive
FROM dbo.Vehicle WHERE Id = @Id";
            return await Connection.QueryFirstOrDefaultAsync<VehicleDTO>(sql, new { Id = id });
        }

        public async Task<bool> ExistsByVehicleNoAsync(string vehicleNo)
        {
            const string sql = @"SELECT 1 FROM dbo.Vehicle WHERE LTRIM(RTRIM(VehicleNo)) = LTRIM(RTRIM(@vehicleNo))";
            var exists = await Connection.ExecuteScalarAsync<int?>(sql, new { vehicleNo });
            return exists.HasValue;
        }

        public async Task<int> CreateAsync(Vehicle v)
        {
            var sql = @"
INSERT INTO dbo.Vehicle (VehicleNo, VehicleType, Capacity, CapacityUom, IsActive, CreatedBy)
OUTPUT INSERTED.Id
VALUES (@VehicleNo, @VehicleType, @Capacity, @CapacityUom, 1, @CreatedBy)";
            return await Connection.ExecuteScalarAsync<int>(sql, v);
        }

        public async Task UpdateAsync(Vehicle v)
        {
            var sql = @"
UPDATE dbo.Vehicle
SET VehicleNo   = @VehicleNo,
    VehicleType = @VehicleType,
    Capacity    = @Capacity,
    CapacityUom = @CapacityUom,
    UpdatedBy   = @UpdatedBy,
    UpdatedOn   = SYSUTCDATETIME()
WHERE Id = @Id";
            await Connection.ExecuteAsync(sql, v);
        }

        public async Task DeactivateAsync(int id)
        {
            const string sql = @"UPDATE dbo.Vehicle SET IsActive = 0, UpdatedOn = SYSUTCDATETIME() WHERE Id = @Id";
            await Connection.ExecuteAsync(sql, new { Id = id });
        }
    }
}
