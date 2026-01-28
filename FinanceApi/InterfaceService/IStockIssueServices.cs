using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IStockIssueServices
    {
        Task<IEnumerable<StockIssuesDTO>> GetAllAsync();
        Task<int> CreateAsync(StockIssues StockIssuesDTO);
        Task<StockIssuesDTO> GetById(long id);
        Task UpdateAsync(StockIssues StockIssuesDTO);

        Task DeleteAsync(int id);
    }
}

