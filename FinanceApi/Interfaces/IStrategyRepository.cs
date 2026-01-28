using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IStrategyRepository
    {
        Task<IEnumerable<Strategy>> GetAllAsync();
        Task<Strategy> GetByIdAsync(int id);
        Task<int> CreateAsync(Strategy strategy);
        Task UpdateAsync(Strategy strategy);
        Task DeactivateAsync(int id);
    }
}
