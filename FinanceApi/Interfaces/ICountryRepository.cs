using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ICountryRepository
    {
        Task<IEnumerable<Country>> GetAllAsync();
        Task<Country> GetByIdAsync(int id);
        Task<int> CreateAsync(Country country);
        Task UpdateAsync(Country country);
        Task DeactivateAsync(int id);

        Task<Country> GetByNameAsync(string name);

        Task<bool> NameExistsAsync(string CountryName, int excludeId);
    }
}
