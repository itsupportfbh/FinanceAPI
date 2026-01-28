using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class BinRepository : DynamicRepository,IBinRepository
    {
        public BinRepository(IDbConnectionFactory connectionFactory)
 : base(connectionFactory)
        {
        }


        public async Task<IEnumerable<BinDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT 
                    ID,
                    CreatedBy,
                    CreatedDate,
                    UpdatedBy,
                    UpdatedDate,
                    IsActive,
                    BinName
                FROM Bin
where isActive = 1
                ORDER BY ID
";

            return await Connection.QueryAsync<BinDTO>(query);
        }


        public async Task<BinDTO> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM Bin WHERE Id = @Id";

            return await Connection.QuerySingleAsync<BinDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Bin BinDTO)
        {
            const string query = @"INSERT INTO Bin (BinName,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@BinName,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, BinDTO);
        }


        public async Task UpdateAsync(Bin BinDTO)
        {
            const string query = "UPDATE Bin SET BinName = @BinName WHERE Id = @Id";
            await Connection.ExecuteAsync(query, BinDTO);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Bin SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
