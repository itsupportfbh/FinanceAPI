using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class ItemTypeRepository : DynamicRepository, IItemTypeRepository
    {
        public ItemTypeRepository(IDbConnectionFactory connectionFactory)
   : base(connectionFactory)
        {
        }
        public async Task<IEnumerable<ItemTypeDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT * from ItemType";

            return await Connection.QueryAsync<ItemTypeDTO>(query);
        }


        public async Task<ItemTypeDTO> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM ItemType WHERE Id = @Id";

            return await Connection.QuerySingleAsync<ItemTypeDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(ItemType ItemTypeDTO)
        {
            const string query = @"INSERT INTO ItemType (ItemTypeName,Description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@ItemTypeName,@Description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, ItemTypeDTO);
        }


        public async Task UpdateAsync(ItemType ItemTypeDTO)
        {
            const string query = "UPDATE ItemType SET ItemTypeName = @ItemTypeName, Description = @Description WHERE Id = @Id";
            await Connection.ExecuteAsync(query, ItemTypeDTO);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE ItemType SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }

    }
}
