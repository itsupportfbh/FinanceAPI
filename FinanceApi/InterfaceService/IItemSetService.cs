using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IItemSetService
    {
        Task<IEnumerable<ItemSetDTO>> GetAllAsync();
        Task<int> CreateAsync(ItemSet itemSet);
        Task<ItemSetDTO> GetById(int id);
        Task UpdateAsync(ItemSet itemSet);
        Task DeleteAsync(int id);
    }
}
