using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ILocationService
    {
        Task<IEnumerable<LocationDto>> GetAllAsync();
        Task<IEnumerable<LocationDto>> GetAllLocationDetails();
        Task<LocationDto> GetById(int id);
        Task<int> CreateAsync(Location location);
        Task UpdateAsync(Location location);
        Task DeleteAsync(int id);

        Task<IEnumerable<LocationDto>> GetLocationByCountryId(int id);
    }
}
