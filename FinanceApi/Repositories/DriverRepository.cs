using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class DriverRepository : DynamicRepository,IDriverRepository
    {
        public DriverRepository(IDbConnectionFactory connectionFactory)
             : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<DriverDTO>> GetAllAsync()
        {
            const string query = @"
            SELECT 
                Id,
                DriverName,
                MobileNumber,
                LicenseNumber,
                LicenseExpiryDate,
                NricOrId,
                IsActive,
                CreatedDate,
                CreatedBy,
                UpdatedDate,
                UpdatedBy
            FROM Driver
            WHERE IsActive = 1
            ORDER BY Id";

            return await Connection.QueryAsync<DriverDTO>(query);
        }

        // Get Driver by ID
        public async Task<DriverDTO> GetByIdAsync(int id)
        {
            const string query = "SELECT * FROM Driver WHERE Id = @Id";
            return await Connection.QuerySingleAsync<DriverDTO>(query, new { Id = id });
        }

        // Create new driver
        public async Task<int> CreateAsync(Driver dto)
        {
            const string query = @"
        INSERT INTO Driver 
            (DriverName, MobileNumber, LicenseNumber, LicenseExpiryDate, NricOrId, CreatedBy, CreatedDate, IsActive)
        OUTPUT INSERTED.Id
        VALUES 
            (@DriverName, @MobileNumber, @LicenseNumber, @LicenseExpiryDate, @NricOrId, @CreatedBy, GETUTCDATE(), 1)";

            return await Connection.QueryFirstAsync<int>(query, dto);
        }

        // Update driver details
        public async Task UpdateAsync(Driver dto)
        {
            const string query = @"
        UPDATE Driver SET
            DriverName = @DriverName,
            MobileNumber = @MobileNumber,
            LicenseNumber = @LicenseNumber,
            LicenseExpiryDate = @LicenseExpiryDate,
            NricOrId = @NricOrId,
            UpdatedBy = @UpdatedBy,
            UpdatedDate = GETUTCDATE()
        WHERE Id = @Id";

            await Connection.ExecuteAsync(query, dto);
        }

        // Soft Delete (Deactivate)
        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Driver SET IsActive = 0, UpdatedDate = GETUTCDATE() WHERE Id = @Id";
            await Connection.ExecuteAsync(query, new { Id = id });
        }
    }
}
