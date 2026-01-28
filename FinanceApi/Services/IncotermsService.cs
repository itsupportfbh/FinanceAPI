using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;

namespace FinanceApi.Services
{
    public class IncotermsService : IIncotermsService
    {
        private readonly IIncotermsRepository _repository;

        public IncotermsService (IIncotermsRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<IncotermsDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(IncotermsDTO incotermsDTO)
        {
            return await _repository.CreateAsync(incotermsDTO);

        }

        public async Task<IncotermsDTO> GetById(long id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateLicense(IncotermsDTO incotermsDTO)
        {
            return _repository.UpdateAsync(incotermsDTO);
        }


        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }

        public async Task<IncotermsDTO> GetByName(string name)
        {
            return await _repository.GetByNameAsync(name);
        }

        public async Task<bool> NameExistsAsync(string Name, long excludeId)
        {
            return await _repository.NameExistsAsync(Name, excludeId);
        }

    }
}
