using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface ICostingMethodService
    {
        Task<IEnumerable<CostingMethodDTO>> GetAllAsync();
        Task<CostingMethodDTO> GetById(int id);
        Task<int> CreateAsync(CostingMethod costingMethod);
        Task UpdateAsync(CostingMethod costingMethod);
        Task DeleteAsync(int id);
    }
}
