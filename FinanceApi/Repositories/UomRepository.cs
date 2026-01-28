using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;

namespace FinanceApi.Repositories
{
    // Repository (Dapper style like CurrencyRepository)
    public class UomRepository : DynamicRepository, IUomRepository
    {
        public UomRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<UomDTO>> GetAllAsync()
        {
            const string query = @"SELECT * FROM Uom WHERE IsActive = 1 ORDER BY Id";
            return await Connection.QueryAsync<UomDTO>(query);
        }

        public async Task<UomDTO> GetByIdAsync(int id)
        {
            const string query = @"SELECT * FROM Uom WHERE Id = @Id";
            return await Connection.QuerySingleAsync<UomDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Uom uom)
        {
            const string query = @"
INSERT INTO Uom (Name, Description, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
OUTPUT INSERTED.Id
VALUES (@Name, @Description, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive)";
            return await Connection.QueryFirstAsync<int>(query, uom);
        }

        public async Task UpdateAsync(Uom uom)
        {
            const string query = @"
UPDATE Uom
SET Name = @Name,
    Description = @Description,
    UpdatedBy = @UpdatedBy,
    UpdatedDate = @UpdatedDate
WHERE Id = @Id";
            await Connection.ExecuteAsync(query, uom);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = @"UPDATE Uom SET IsActive = 0 WHERE Id = @Id";
            await Connection.ExecuteAsync(query, new { Id = id });
        }


        public async Task<UomDTO> GetByNameAsync(string name)
        {

            const string query = "SELECT * FROM Uom WHERE Name = @Name and IsActive=1";

            return await Connection.QuerySingleOrDefaultAsync<UomDTO>(query, new { Name = name });
        }

        public async Task<bool> NameExistsAsync(string Name, int excludeId)
        {
            const string sql = @"
        SELECT 1
        FROM UOM
        WHERE IsActive = 1
          AND Id <> @excludeId
          AND UPPER(LTRIM(RTRIM(Name))) = UPPER(LTRIM(RTRIM(@Name)))";

            var found = await Connection.QueryFirstOrDefaultAsync<int?>(sql, new { Name, excludeId });
            return found.HasValue;
        }
    }

}
