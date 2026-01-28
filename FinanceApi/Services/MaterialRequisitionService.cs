using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class MaterialRequisitionService : IMaterialRequisitionService
    {
        private readonly IMaterialRequisitionRepository _repository;

        public MaterialRequisitionService (IMaterialRequisitionRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<MaterialRequisitionDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        } 

        public async Task<MaterialRequisitionDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }
        public async Task<int> CreateAsync(MaterialRequisition mrq)
        {
            return await _repository.CreateAsync(mrq);

        }

        public Task UpdateAsync(MaterialRequisition mrq)
        {
            return _repository.UpdateAsync(mrq);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
