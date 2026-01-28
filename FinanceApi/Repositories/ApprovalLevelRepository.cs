using FinanceApi.Data;
using Dapper;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class ApprovalLevelRepository : DynamicRepository, IApprovalLevelRepository
    {
        private readonly ApplicationDbContext _context;

        public ApprovalLevelRepository(IDbConnectionFactory connectionFactory)
        : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<ApprovalLevelDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT * from ApprovalLevel";

            return await Connection.QueryAsync<ApprovalLevelDTO>(query);
        }


        public async Task<ApprovalLevelDTO> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM ApprovalLevel WHERE Id = @Id";

            return await Connection.QuerySingleAsync<ApprovalLevelDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(ApprovalLevel approvalLevel)
        {
            const string query = @"INSERT INTO ApprovalLevel (Name,Description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@Name,@Description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, approvalLevel);
        }


        public async Task UpdateAsync(ApprovalLevel approvalLevel)
        {
            const string query = "UPDATE ApprovalLevel SET Name = @Name, Description =@Description WHERE Id = @Id";
            await Connection.ExecuteAsync(query, approvalLevel);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE ApprovalLevel SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
        public async Task<ApprovalLevelDTO> GetByNameAsync(string name)
        {

            const string query = "SELECT * FROM ApprovalLevel WHERE Name = @name and IsActive=1";

            return await Connection.QuerySingleOrDefaultAsync<ApprovalLevelDTO>(query, new {Name = name });
        }
        public async Task<bool> NameExistsAsync(string name, int excludeId)
        {
            const string sql = @"
        SELECT 1
        FROM ApprovalLevel
        WHERE IsActive = 1
          AND Id <> @excludeId
          AND UPPER(LTRIM(RTRIM(Name))) = UPPER(LTRIM(RTRIM(@name)))";

            var found = await Connection.QueryFirstOrDefaultAsync<int?>(sql, new { name, excludeId });
            return found.HasValue;
        }
    }
}
