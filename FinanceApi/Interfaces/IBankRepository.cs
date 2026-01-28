using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IBankRepository
    {
        Task<IEnumerable<BankDto>> GetAllAsync();
        Task<BankDto> GetByIdAsync(long id);
        Task<int> CreateAsync(Bank BankDTO);
        Task UpdateAsync(Bank BankDTO);
        Task DeactivateAsync(int id);
    }
}
