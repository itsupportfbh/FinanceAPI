using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IItemService
    {
        Task<IEnumerable<ItemDto>> GetAllAsync();
        Task<int> CreateAsync(Item item);
        Task<ItemDto?> GetById(int id);
        Task UpdateAsync(Item item);
        Task DeleteItem(int id);
        Task<bool> ExistsInItemMasterAsync(string code);
    }

}
