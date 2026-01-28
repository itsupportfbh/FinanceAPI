using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IcostingMethodRepository
    {
        Task<IEnumerable<CostingMethodDTO>> GetAllAsync();
        Task<CostingMethodDTO> GetByIdAsync(long id);

        Task<int> CreateAsync(CostingMethod costingMethod);

        Task UpdateAsync(CostingMethod costingMethod);

        Task DeactivateAsync(int id);
    }
}
