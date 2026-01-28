using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class RegionService : IRegionService
    {
        private readonly IRegionRepository _repository;

        public RegionService(IRegionRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<Region>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<Region?> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<int> CreateAsync(Region region)
        {

            return await _repository.CreateAsync(region);
        }

        public async Task UpdateAsync(Region region)
        {
             await _repository.UpdateAsync(region);
        }

        public async Task DeleteLicense(int id)
        {
             await _repository.DeactivateAsync(id);
        }
    }
}
