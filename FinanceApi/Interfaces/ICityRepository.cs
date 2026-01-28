using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ICityRepository
    {
        Task<IEnumerable<CityDto>> GetAllAsync();
        Task<CityDto> GetByIdAsync(long id);
        Task<int> CreateAsync(City city);
        Task UpdateAsync(City city);
        Task DeactivateAsync(int id);

        Task<IEnumerable<CityDto>> GetStateWithCountryId(int id);

        Task<IEnumerable<CityDto>> GetCityWithStateId(int id);
        Task<CityDto?> GetByNameInCountryAsync(string name, int countryId);
        Task<bool> NameExistsAsync(string name, int countryId, int excludeId);
    }
}
