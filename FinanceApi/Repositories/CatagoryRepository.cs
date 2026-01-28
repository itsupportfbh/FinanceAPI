using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class CatagoryRepository : DynamicRepository, ICatagoryRepository
    {
        public CatagoryRepository(IDbConnectionFactory connectionFactory)
       : base(connectionFactory)
        {
        }


        public async Task<IEnumerable<CatagoryDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Catagory where isActive = 1";

            return await Connection.QueryAsync<CatagoryDTO>(query);
        }


        public async Task<CatagoryDTO> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM Catagory WHERE Id = @Id";

            return await Connection.QuerySingleAsync<CatagoryDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Catagory catagoryDTO)
        {
            const string query = @"INSERT INTO Catagory (CatagoryName,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@CatagoryName,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, catagoryDTO);
        } 


        public async Task UpdateAsync(CatagoryDTO catagoryDTO)
        {
            const string query = "UPDATE Catagory SET CatagoryName = @CatagoryName WHERE Id = @Id";
            await Connection.ExecuteAsync(query, catagoryDTO);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Catagory SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }


    }
}
