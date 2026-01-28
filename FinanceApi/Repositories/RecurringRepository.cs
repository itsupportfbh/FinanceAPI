using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class RecurringRepository : DynamicRepository, IRecurringRepository
    {
        private readonly ApplicationDbContext _context;

        public RecurringRepository(IDbConnectionFactory connectionFactory)
       : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<Recurring>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Recurring Where isActive = 'true' ";

            return await Connection.QueryAsync<Recurring>(query);
        }


        public async Task<Recurring> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM Recurring WHERE Id = @Id";

            return await Connection.QuerySingleAsync<Recurring>(query, new { Id = id });
        }
        public async Task<int> CreateAsync(Recurring recurring)
        {
            const string query = @"
        INSERT INTO Recurring 
            (RecurringName, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive) 
        OUTPUT INSERTED.Id 
        VALUES 
            (@RecurringName, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive)
    ";

            var parameters = new DynamicParameters();
            parameters.Add("@RecurringName", recurring.RecurringName);
            parameters.Add("@CreatedBy", recurring.CreatedBy);
            parameters.Add("@CreatedDate", recurring.CreatedDate);
            parameters.Add("@UpdatedBy", recurring.UpdatedBy);
            parameters.Add("@UpdatedDate", recurring.UpdatedDate);
            parameters.Add("@IsActive", recurring.IsActive);

            return await Connection.QueryFirstAsync<int>(query, parameters);
        }




        public async Task UpdateAsync(Recurring recurring)
        {
            const string query = "UPDATE Recurring SET RecurringName = @RecurringName WHERE Id = @Id";
            await Connection.ExecuteAsync(query, recurring);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Recurring SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }

        public async Task<RecurringDTO> GetByNameAsync(string name)
        {

            const string query = "SELECT * FROM Recurring WHERE RecurringName = @RecurringName and IsActive=1";

            return await Connection.QuerySingleOrDefaultAsync<RecurringDTO>(query, new { RecurringName = name });
        }
    }
}
