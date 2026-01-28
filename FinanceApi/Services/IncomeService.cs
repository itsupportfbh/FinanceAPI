using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class IncomeService : IIncomeService
    {
        private readonly IIncomeRepository _repository;

        public IncomeService(IIncomeRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<Income>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<Income?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<int> CreateAsync(Income income)
        {

            return await _repository.CreateAsync(income);
        }

        public async Task UpdateAsync(Income income)
        {
            await _repository.UpdateAsync(income);
        }

        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
