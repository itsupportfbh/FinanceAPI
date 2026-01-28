using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IRegionRepository
    {
        Task<IEnumerable<Region>> GetAllAsync();
        Task<Region> GetByIdAsync(int id);
        Task<int> CreateAsync(Region region);
        Task UpdateAsync(Region region);
        Task DeactivateAsync(int id);
    }
}
