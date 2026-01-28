using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using FinanceApi.Repositories;

namespace FinanceApi.Services
{
    public class ChartOfAccountService : IChartOfAccountService
    {
        private readonly IChartOfAccountRepository _repository;

        public ChartOfAccountService(IChartOfAccountRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<ChartOfAccountDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(ChartOfAccount chartOfAccount)
        {
            return await _repository.CreateAsync(chartOfAccount);
        }

        public async Task<ChartOfAccountDTO?> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(ChartOfAccount chartOfAccount)
        {
            return _repository.UpdateAsync(chartOfAccount);
        }

        public async Task DeleteChartOfAccount(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }

}
