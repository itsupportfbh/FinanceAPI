using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using Dapper;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class OpeningBalanceRepository : DynamicRepository, IOpeningBalanceRepository
    {
        private readonly ApplicationDbContext _context;

        public OpeningBalanceRepository(IDbConnectionFactory connectionFactory)
       : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<OpeningBalanceDto>> GetAllAsync()
        {
            const string query = @"
                SELECT * from OpeningBalance Where isActive = 'true' ";

            return await Connection.QueryAsync<OpeningBalanceDto>(query);
        }


        public async Task<OpeningBalanceDto> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM OpeningBalance WHERE Id = @Id";

            return await Connection.QuerySingleAsync<OpeningBalanceDto>(query, new { Id = id });
        }
        public async Task<int> CreateAsync(OpeningBalance OpeningBalance)
        {
            const string query = @"
        INSERT INTO OpeningBalance 
            (OpeningBalanceAmount, BudgetLineId , CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive) 
        OUTPUT INSERTED.Id 
        VALUES 
            (@OpeningBalanceAmount, @BudgetLineId, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive)
    ";

            var parameters = new DynamicParameters();
            parameters.Add("@OpeningBalanceAmount", OpeningBalance.OpeningBalanceAmount);
            parameters.Add("@BudgetLineId", OpeningBalance.BudgetLineId);
            parameters.Add("@CreatedBy", OpeningBalance.CreatedBy);
            parameters.Add("@CreatedDate", OpeningBalance.CreatedDate);
            parameters.Add("@UpdatedBy", OpeningBalance.UpdatedBy);
            parameters.Add("@UpdatedDate", OpeningBalance.UpdatedDate);
            parameters.Add("@IsActive", OpeningBalance.IsActive);

            return await Connection.QueryFirstAsync<int>(query, parameters);
        }




        public async Task UpdateAsync(OpeningBalance OpeningBalance)
        {
            const string query = "UPDATE OpeningBalance SET BudgetLineId = @BudgetLineId, OpeningBalanceAmount = @OpeningBalanceAmount WHERE Id = @Id";
            await Connection.ExecuteAsync(query, OpeningBalance);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE OpeningBalance SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
