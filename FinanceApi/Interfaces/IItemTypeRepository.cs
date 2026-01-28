using FinanceApi.ModelDTO;
using FinanceApi.Models;
using FinanceApi.Repositories;

namespace FinanceApi.Interfaces
{
    public interface IItemTypeRepository 

    {
        Task<IEnumerable<ItemTypeDTO>> GetAllAsync();
        Task<ItemTypeDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(ItemType ItemTypeDTO);
        Task UpdateAsync(ItemType ItemTypeDTO);
        Task DeactivateAsync(int id);
    }
}
