using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class OpeningBalanceService : IOpeningBalanceService
    {
        private readonly IOpeningBalanceRepository _repository;

        public OpeningBalanceService(IOpeningBalanceRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<OpeningBalanceDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(OpeningBalance OpeningBalance)
        {
            return await _repository.CreateAsync(OpeningBalance);

        }

        public async Task<OpeningBalanceDto> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(OpeningBalance OpeningBalance)
        {
            return _repository.UpdateAsync(OpeningBalance);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
