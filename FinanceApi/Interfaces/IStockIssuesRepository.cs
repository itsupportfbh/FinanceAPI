using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IStockIssuesRepository
    {
        Task<IEnumerable<StockIssuesDTO>> GetAllAsync();
        Task<StockIssuesDTO> GetByIdAsync(long id);

        Task<int> CreateAsync(StockIssues StockIssuesDTO);

        Task UpdateAsync(StockIssues StockIssuesDTO);

        Task DeactivateAsync(int id);
    }
}
