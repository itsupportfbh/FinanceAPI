using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IBankService
    {
        Task<IEnumerable<BankDto>> GetAllAsync();
        Task<int> CreateAsync(Bank BankDto);
        Task<BankDto> GetById(long id);
        Task UpdateAsync(Bank BankDto);
        Task DeleteAsync(int id);
    }
}
