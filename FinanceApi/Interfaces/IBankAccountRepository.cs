using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface IBankAccountRepository
    {
        Task<IEnumerable<BankAccountBalanceDto>> GetBankAccountsAsync();
    }
}
