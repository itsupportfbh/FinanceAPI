using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class WarehouseService : IWarehouseService
    {
        private readonly IWarehouseRepository _repository;

        public WarehouseService(IWarehouseRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<WarehouseDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<WarehouseDto?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<int> CreateAsync(Warehouse warehouse)
        {
            return await _repository.CreateAsync(warehouse);
        }

        public async Task UpdateAsync(Warehouse warehouse)
        {
            await _repository.UpdateAsync(warehouse);
        }

        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }
        public async Task<IEnumerable<WarehouseDto>> GetBinNameByIdAsync(int id)
        {
            return await _repository.GetBinNameByIdAsync(id);
        }

        public async Task<IEnumerable<WarehouseDto>> GetNameByWarehouseAsync(string name)
        {
            return await _repository.GetNameByWarehouseAsync(name);
        }
    }
}
