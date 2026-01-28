using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IItemRepository
    {
        Task<IEnumerable<ItemDto>> GetAllAsync();
        Task<ItemDto?> GetByIdAsync(int id);
        Task<int> CreateAsync(Item item);
        Task UpdateAsync(Item item);
        Task DeactivateAsync(int id);

        Task<bool> ExistsInItemMasterAsync(string itemCode);
    }

}
