using FinanceApi.Models;
using FinanceApi.Data;
using Microsoft.EntityFrameworkCore;
using FinanceApi.Interfaces;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using Dapper;

namespace FinanceApi.Repositories
{
    public class DeductionRepository : DynamicRepository,IDeductionRepository
    {
        private readonly ApplicationDbContext _context;

        public DeductionRepository(IDbConnectionFactory connectionFactory)
          : base(connectionFactory)
        {

        }

        public async Task<IEnumerable<Deduction>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Deduction Where isActive = 'true' ";

            return await Connection.QueryAsync<Deduction>(query);
        }

        public async Task<Deduction?> GetByIdAsync(int id)
        {
            const string query = "SELECT * FROM Deduction WHERE Id = @Id";

            return await Connection.QuerySingleAsync<Deduction>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Deduction deduction)
        {
            const string query = @"INSERT INTO Deduction (Name,Description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@Name,@Description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, deduction);
        }

        public async Task UpdateAsync(Deduction updatedDeduction)
        {
            const string query = "UPDATE Deduction SET Name = @Name,Description = @Description WHERE Id = @Id";
            await Connection.ExecuteAsync(query, updatedDeduction);
        }


        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Deduction SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
