using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class StockIssuesServices : IStockIssueServices
    {
        private readonly IStockIssuesRepository _repository;

        public StockIssuesServices(IStockIssuesRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<StockIssuesDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(StockIssues StockIssuesDTO)
        {
            return await _repository.CreateAsync(StockIssuesDTO);

        }

        public async Task<StockIssuesDTO> GetById(long id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(StockIssues StockIssuesDTO)
        {
            return _repository.UpdateAsync(StockIssuesDTO);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
