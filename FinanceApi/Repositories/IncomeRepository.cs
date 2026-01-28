using FinanceApi.Models;
using FinanceApi.Data;
using Microsoft.EntityFrameworkCore;
using FinanceApi.Interfaces;
using Dapper;


namespace FinanceApi.Repositories
{
    public class IncomeRepository : DynamicRepository,IIncomeRepository
    {
        private readonly ApplicationDbContext _context;

        public IncomeRepository(IDbConnectionFactory connectionFactory)
        : base(connectionFactory)
        {

        }
        public async Task<IEnumerable<Income>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Income Where isActive = 'true' ";

            return await Connection.QueryAsync<Income>(query);
        }

        public async Task<Income?> GetByIdAsync(int id)
        {
            const string query = "SELECT * FROM Income WHERE Id = @Id";
            return await Connection.QuerySingleAsync<Income>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Income income)
        {
            const string query = @"INSERT INTO Income (Name,Description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@Name,@Description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, income);
        }

        public async Task  UpdateAsync(Income updatedIncome)
        {
            const string query = "UPDATE Income SET Name = @Name,Description = @Description WHERE Id = @Id";
            await Connection.ExecuteAsync(query, updatedIncome);
        }


        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Income SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
