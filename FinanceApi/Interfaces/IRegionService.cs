using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IRegionService
    {    

        Task<IEnumerable<Region>> GetAllAsync();
        Task<Region> GetById(int id);
        Task<int> CreateAsync(Region region);
        Task UpdateAsync(Region region);
        Task DeleteLicense(int id);
    }
}
