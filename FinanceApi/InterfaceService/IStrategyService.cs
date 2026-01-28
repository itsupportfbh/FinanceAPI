using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IStrategyService
    {
        Task<IEnumerable<Strategy>> GetAllAsync();
        Task<Strategy> GetById(int id);
        Task<int> CreateAsync(Strategy strategy);
        Task UpdateAsync(Strategy strategy);
        Task DeleteLicense(int id);
    }
}
