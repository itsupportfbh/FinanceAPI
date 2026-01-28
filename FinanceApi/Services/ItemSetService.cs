using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class ItemSetService : IItemSetService
    {
        private readonly IItemSetRepository _repository;

        public ItemSetService(IItemSetRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<ItemSetDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(ItemSet itemSet)
        {
            return await _repository.CreateAsync(itemSet);

        }

        public async Task<ItemSetDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(ItemSet itemSet)
        {
            return _repository.UpdateAsync(itemSet);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }

    }
}
