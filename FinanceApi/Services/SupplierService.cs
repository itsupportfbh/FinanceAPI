using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class SupplierService : ISupplierService
    {
        private readonly ISupplierRepository _repository;

        public SupplierService(ISupplierRepository repository)
        {
            _repository = repository;
        }

        public async Task<List<SupplierDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<SupplierDto?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<Supplier> CreateAsync(Supplier supplier)
        {
            return await _repository.CreateAsync(supplier);
        }

        public async Task<Supplier?> UpdateAsync(int id, Supplier supplier)
        {
            return await _repository.UpdateAsync(id, supplier);
        }

        public async Task<bool> DeleteAsync(int id)
        {
            return await _repository.DeleteAsync(id);
        }
    }
}
