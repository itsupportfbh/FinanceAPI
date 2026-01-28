using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;


namespace FinanceApi.Repositories
{
    public class SupplierGroupRepository : DynamicRepository,ISupplierGroupsRepository
    {
        private readonly ApplicationDbContext _context;

        public SupplierGroupRepository(IDbConnectionFactory connectionFactory)
      : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<SupplierGroupDTO>> GetAllAsync()
        {
            const string query = @" SELECT * from SupplierGroups Where isActive = 1";

            return await Connection.QueryAsync<SupplierGroupDTO>(query);
        }


        public async Task<SupplierGroupDTO> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM SupplierGroups WHERE Id = @Id";

            return await Connection.QuerySingleAsync<SupplierGroupDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(SupplierGroups supplierGroups)
        {
            const string query = @"INSERT INTO SupplierGroups (Name,Description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@Name,@Description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, supplierGroups);
        }


        public async Task UpdateAsync(SupplierGroups supplierGroups)
        {
            const string query = "UPDATE SupplierGroups SET Name = @Name, Description = @Description WHERE Id = @Id";
            await Connection.ExecuteAsync(query, supplierGroups);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE SupplierGroups SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
