using FinanceApi.Interfaces;
using FinanceApi.Models;
using FinanceApi.Repositories;

namespace FinanceApi.Services
{
    public class PurchaseService : IPurchaseService
    {
        private readonly IPurchaseRepository _repository;

        public PurchaseService(IPurchaseRepository repository)
        {
            _repository = repository;
        }

        public async Task<List<PurchasesDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<PurchasesDto?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<Purchases> CreateAsync(Purchases purchases)
        {
            // Example validation logic you might add later
            if (purchases.LineItems == null || !purchases.LineItems.Any())
                throw new ArgumentException("At least one line item is required.");

            return await _repository.CreateAsync(purchases);
        }

        public async Task<Purchases?> UpdateAsync(int id, Purchases purchases)
        {
            return await _repository.UpdateAsync(id, purchases);
        }

        public async Task<bool> DeleteAsync(int id)
        {
            return await _repository.DeleteAsync(id);
        }
    }
}
