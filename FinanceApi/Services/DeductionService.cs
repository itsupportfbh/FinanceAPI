using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class DeductionService : IDeductionService
    {

        private readonly IDeductionRepository _repository;

        public DeductionService(IDeductionRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<Deduction>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<Deduction?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<int> CreateAsync(Deduction deduction)
        {

            return await _repository.CreateAsync(deduction);
        }

        public async Task UpdateAsync(Deduction deduction)
        {
            await _repository.UpdateAsync(deduction);
        }

        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
