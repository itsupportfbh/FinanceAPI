using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class CostingMethodRepository : DynamicRepository,IcostingMethodRepository
    {
        public CostingMethodRepository(IDbConnectionFactory connectionFactory)
       : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<CostingMethodDTO>> GetAllAsync()
        {
            const string query = @" SELECT * from CostingMethod Where isActive = 1";

            return await Connection.QueryAsync<CostingMethodDTO>(query);
        }


        public async Task<CostingMethodDTO> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM CostingMethod WHERE Id = @Id";

            return await Connection.QuerySingleAsync<CostingMethodDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(CostingMethod costingMethod)
        {
            const string query = @"INSERT INTO CostingMethod (CostingName,description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@CostingName,@description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, costingMethod);
        }


        public async Task UpdateAsync(CostingMethod costingMethod)
        {
            const string query = @"UPDATE CostingMethod 
                       SET CostingName = @CostingName, description = @description
                       WHERE Id = @Id";
            await Connection.ExecuteAsync(query, costingMethod);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE CostingMethod SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
