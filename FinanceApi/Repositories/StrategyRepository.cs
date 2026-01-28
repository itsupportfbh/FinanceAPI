using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class StrategyRepository : DynamicRepository, IStrategyRepository
    {
        private readonly ApplicationDbContext _context;

        public StrategyRepository(IDbConnectionFactory connectionFactory)
       : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<Strategy>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Strategy Where isActive = 'true' ";

            return await Connection.QueryAsync<Strategy>(query);
        }


        public async Task<Strategy> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM Strategy WHERE Id = @Id";

            return await Connection.QuerySingleAsync<Strategy>(query, new { Id = id });
        }
        public async Task<int> CreateAsync(Strategy strategy)
        {
            const string query = @"
        INSERT INTO Strategy 
            (StrategyName, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive) 
        OUTPUT INSERTED.Id 
        VALUES 
            (@StrategyName,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive)
    ";

            var parameters = new DynamicParameters();
            parameters.Add("@StrategyName", strategy.StrategyName);
            parameters.Add("@CreatedBy", strategy.CreatedBy);
            parameters.Add("@CreatedDate", strategy.CreatedDate);
            parameters.Add("@UpdatedBy", strategy.UpdatedBy);
            parameters.Add("@UpdatedDate", strategy.UpdatedDate);
            parameters.Add("@IsActive", strategy.IsActive);

            return await Connection.QueryFirstAsync<int>(query, parameters);
        }




        public async Task UpdateAsync(Strategy strategy)
        {
            const string query = "UPDATE Strategy SET StrategyName = @StrategyName WHERE Id = @Id";
            await Connection.ExecuteAsync(query, strategy);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Strategy SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
