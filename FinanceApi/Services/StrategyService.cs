using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class StrategyService : IStrategyService
    {
        private readonly IStrategyRepository _repository;

        public StrategyService(IStrategyRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<Strategy>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(Strategy strategy)
        {
            return await _repository.CreateAsync(strategy);

        }

        public async Task<Strategy> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(Strategy strategy)
        {
            return _repository.UpdateAsync(strategy);
        }


        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
