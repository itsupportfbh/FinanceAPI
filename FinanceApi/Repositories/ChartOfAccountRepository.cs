using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;

namespace FinanceApi.Repositories
{
    using Dapper;
    using FinanceApi.ModelDTO;

    public class ChartOfAccountRepository : DynamicRepository, IChartOfAccountRepository
    {
        public ChartOfAccountRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<ChartOfAccountDTO>> GetAllAsync()
        {
            const string query = @"SELECT * FROM ChartOfAccount";
            return await Connection.QueryAsync<ChartOfAccountDTO>(query);
        }

        public async Task<ChartOfAccountDTO?> GetByIdAsync(int id)
        {
            const string query = @"SELECT * FROM ChartOfAccount WHERE Id = @Id";
            // Use QuerySingleOrDefault to avoid throwing when not found
            return await Connection.QuerySingleOrDefaultAsync<ChartOfAccountDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(ChartOfAccount coa)
        {
            const string query = @"
INSERT INTO ChartOfAccount
(HeadCode, HeadLevel, HeadName, HeadType, HeadCodeName, IsGl, IsTransaction, ParentHead, PHeadName,
  CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
OUTPUT INSERTED.Id
VALUES
(@HeadCode, @HeadLevel, @HeadName, @HeadType, @HeadCodeName, @IsGl, @IsTransaction, @ParentHead, @PHeadName,
  @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive)";
            return await Connection.QueryFirstAsync<int>(query, coa);
        }

        public async Task UpdateAsync(ChartOfAccount coa)
        {
            const string query = @"
UPDATE ChartOfAccount
SET HeadCode = @HeadCode,
    HeadLevel = @HeadLevel,
    HeadName = @HeadName,
    HeadType = @HeadType,
    HeadCodeName = @HeadCodeName,
    IsGl = @IsGl,
    IsTransaction = @IsTransaction,
    ParentHead = @ParentHead,
    PHeadName = @PHeadName,
    UpdatedBy = @UpdatedBy,
    UpdatedDate = @UpdatedDate
WHERE Id = @Id";
            await Connection.ExecuteAsync(query, coa);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = @"UPDATE ChartOfAccount SET IsActive = 0 WHERE Id = @Id";
            await Connection.ExecuteAsync(query, new { Id = id });
        }
    }

}
