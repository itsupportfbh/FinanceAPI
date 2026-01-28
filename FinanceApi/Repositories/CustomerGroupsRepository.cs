using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class CustomerGroupsRepository : DynamicRepository,ICustomerGroupsRepository
    {
        private readonly ApplicationDbContext _context;

        public CustomerGroupsRepository(IDbConnectionFactory connectionFactory)
      : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<CustomerGroupsDTO>> GetAllAsync()
        {
            const string query = @" SELECT * from CustomerGroups Where isActive = 1";

            return await Connection.QueryAsync<CustomerGroupsDTO>(query);
        }


        public async Task<CustomerGroupsDTO> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM CustomerGroups WHERE Id = @Id";

            return await Connection.QuerySingleAsync<CustomerGroupsDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(CustomerGroups customerGroups)
        {
            const string query = @"INSERT INTO CustomerGroups (Name,Description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@Name,@Description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, customerGroups);
        }


        public async Task UpdateAsync(CustomerGroups customerGroups)
        {
            const string query = "UPDATE CustomerGroups SET Name = @Name, Description = @Description WHERE Id = @Id";
            await Connection.ExecuteAsync(query, customerGroups);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE CustomerGroups SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
