using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IItemSetRepository
    {
        Task<IEnumerable<ItemSetDTO>> GetAllAsync();
        Task<ItemSetDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(ItemSet itemSet);
        Task UpdateAsync(ItemSet itemSet);

        Task DeactivateAsync(int id);
    }
}
