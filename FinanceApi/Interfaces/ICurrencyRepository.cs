using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ICurrencyRepository
    {
        Task<IEnumerable<CurrencyDTO>> GetAllAsync();
        Task<CurrencyDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(Currency currencyDTO);
        Task UpdateAsync(Currency currencyDTO);
        Task DeactivateAsync(int id);

        Task<CurrencyDTO> GetByNameAsync(string name);
        Task<bool> NameExistsAsync(string CurrencyName, int excludeId);
    }
}
