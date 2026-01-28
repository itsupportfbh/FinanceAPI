// Repositories/BankAccountRepository.cs
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;

namespace FinanceApi.Repositories
{
    public class BankAccountRepository : DynamicRepository, IBankAccountRepository
    {
        public BankAccountRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<BankAccountBalanceDto>> GetBankAccountsAsync()
        {
            const string sql = @"
SELECT
    b.budgetlineId as Id ,
    b.BankName as HeadName,
    c.Id                 AS HeadId,
    c.HeadCode,
    v.OpeningBalance,
    v.AvailableBalance
FROM dbo.Bank b
LEFT JOIN dbo.ChartOfAccount c
    ON c.Id = b.BudgetLineId          -- link Bank → COA
LEFT JOIN dbo.vw_AccountAvailableBalance v
    ON v.HeadId = c.Id                -- get latest balance
WHERE b.IsActive = 1
ORDER BY b.BankName;
";

            return await Connection.QueryAsync<BankAccountBalanceDto>(sql);
        }

    }
}
