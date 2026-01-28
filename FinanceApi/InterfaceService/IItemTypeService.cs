using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IItemTypeService
    {
        Task<IEnumerable<ItemTypeDTO>> GetAllAsync();
        Task<int> CreateAsync(ItemType ItemTypeDTO);
        Task<ItemTypeDTO> GetById(int id);
        Task UpdateAsync(ItemType ItemTypeDTO);
        Task DeleteAsync(int id);
    }
}
