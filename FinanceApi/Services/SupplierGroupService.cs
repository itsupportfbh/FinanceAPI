using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class SupplierGroupService : ISupplierGroupsService
    {
        private readonly ISupplierGroupsRepository _repository;

        public SupplierGroupService(ISupplierGroupsRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<SupplierGroupDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<SupplierGroupDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }
        public async Task<int> CreateAsync(SupplierGroups supplierGroups)
        {
            return await _repository.CreateAsync(supplierGroups);

        }



        public Task UpdateAsync(SupplierGroups supplierGroups)
        {
            return _repository.UpdateAsync(supplierGroups);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
