using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class ItemTypeService : IItemTypeService
    {
        private readonly IItemTypeRepository _itemTypeRepository;

        public ItemTypeService(IItemTypeRepository itemTypeRepository)
        {
            _itemTypeRepository = itemTypeRepository;
        }

        public async Task<IEnumerable<ItemTypeDTO>> GetAllAsync()
        {
            return await _itemTypeRepository.GetAllAsync();
        }

        public async Task<int> CreateAsync(ItemType ItemTypeDTO)
        {
            return await _itemTypeRepository.CreateAsync(ItemTypeDTO);

        }

        public async Task<ItemTypeDTO> GetById(int id)
        {
            return await _itemTypeRepository.GetByIdAsync(id);
        }

        public Task UpdateAsync(ItemType ItemTypeDTO)
        {
            return _itemTypeRepository.UpdateAsync(ItemTypeDTO);
        }


        public async Task DeleteAsync(int id)
        {
            await _itemTypeRepository.DeactivateAsync(id);
        }

    }
}
