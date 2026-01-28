using FinanceApi.Data;
using FinanceApi.Interfaces;
using Dapper;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class StockIssuesRepository : DynamicRepository,IStockIssuesRepository
    {
        public StockIssuesRepository(IDbConnectionFactory connectionFactory)
   : base(connectionFactory)
        {
        }


        public async Task<IEnumerable<StockIssuesDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT 
                    ID,
                    CreatedBy,
                    CreatedDate,
                    UpdatedBy,
                    UpdatedDate,
                    IsActive,
                    StockIssuesNames
                FROM StockIssues
where isActive = 1
                ORDER BY ID
";

            return await Connection.QueryAsync<StockIssuesDTO>(query);
        }


        public async Task<StockIssuesDTO> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM StockIssues WHERE Id = @Id";

            return await Connection.QuerySingleAsync<StockIssuesDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(StockIssues StockIssuesDTO)
        {
            const string query = @"INSERT INTO StockIssues (StockIssuesNames,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@StockIssuesNames,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, StockIssuesDTO);
        }


        public async Task UpdateAsync(StockIssues StockIssuesDTO)
        {
            const string query = "UPDATE StockIssues SET StockIssuesNames = @StockIssuesNames WHERE Id = @Id";
            await Connection.ExecuteAsync(query, StockIssuesDTO);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE StockIssues SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
