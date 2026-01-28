using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class CostingMethodService : ICostingMethodService
    {
        private readonly IcostingMethodRepository _repository;

        public CostingMethodService(IcostingMethodRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<CostingMethodDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<CostingMethodDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }
        public async Task<int> CreateAsync(CostingMethod costingMethod)
        {
            return await _repository.CreateAsync(costingMethod);

        }



        public Task UpdateAsync(CostingMethod costingMethod)
        {
            return _repository.UpdateAsync(costingMethod);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
