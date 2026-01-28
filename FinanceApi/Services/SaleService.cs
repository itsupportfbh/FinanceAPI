using FinanceApi.Models;
using FinanceApi.Repositories;

namespace FinanceApi.Services
{
    public class SaleService : ISaleService
    {
        private readonly ISaleRepository _repository;

        public SaleService(ISaleRepository repository)
        {
            _repository = repository;
        }

        public async Task<List<SalesDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<SalesDto?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<Sales> CreateAsync(Sales sale)
        {
            // Example validation logic you might add later
            if (sale.LineItems == null || !sale.LineItems.Any())
                throw new ArgumentException("At least one line item is required.");

            return await _repository.CreateAsync(sale);
        }

        public async Task<Sales?> UpdateAsync(int id, Sales sale)
        {
            return await _repository.UpdateAsync(id, sale);
        }

        public async Task<bool> DeleteAsync(int id)
        {
            return await _repository.DeleteAsync(id);
        }
    }
}

