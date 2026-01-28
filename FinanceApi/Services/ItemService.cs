using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class ItemService : IItemService
    {
        private readonly IItemRepository _repository;

        public ItemService(IItemRepository repository)
        {
            _repository = repository;
        }

        public Task<IEnumerable<ItemDto>> GetAllAsync()
        {
            return _repository.GetAllAsync();
        }

        public Task<int> CreateAsync(Item item)
        {
            return _repository.CreateAsync(item);
        }

        public Task<ItemDto?> GetById(int id)
        {
            return _repository.GetByIdAsync(id);
        }

        // Mirror UomService: Update takes the entity; controller sets Id
        public Task UpdateAsync(Item item)
        {
            // Ensure audit fields are set in controller or repository; either is fine.
            return _repository.UpdateAsync(item);
        }

        public Task DeleteItem(int id)
        {
            return _repository.DeactivateAsync(id);
        }

        public Task<bool> ExistsInItemMasterAsync(string code)
        => _repository.ExistsInItemMasterAsync(code);
    }

}
