using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface ICurrencyService
    {
        Task<IEnumerable<CurrencyDTO>> GetAllAsync();
        Task<int> CreateAsync(Currency currencyDTO);
        Task<CurrencyDTO> GetById(int id);
        Task UpdateAsync(Currency currencyDTO);
        Task DeleteLicense(int id);

        Task<CurrencyDTO> GetByName(string name);
        Task<bool> NameExistsAsync(string currencyName, int excludeId);
    }
}
