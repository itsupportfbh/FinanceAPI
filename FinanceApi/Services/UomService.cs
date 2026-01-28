using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class UomService : IUomService
    {
        private readonly IUomRepository _repository;

        public UomService(IUomRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<UomDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(Uom uom)
        {
            return await _repository.CreateAsync(uom);
        }

        public async Task<UomDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(Uom uom)
        {
            return _repository.UpdateAsync(uom);
        }

        public async Task DeleteUom(int id)
        {
            await _repository.DeactivateAsync(id);
        }

        public async Task<UomDTO> GetByName(string name)
        {
            return await _repository.GetByNameAsync(name);
        }

        public async Task<bool> NameExistsAsync(string Name, int excludeId)
        {
            return await _repository.NameExistsAsync(Name, excludeId);
        }
    }
}
