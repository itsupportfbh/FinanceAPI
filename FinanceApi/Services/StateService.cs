using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class StateService : IStateService
    {
        private readonly IStateRepository _repository;

        public StateService(IStateRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<StateDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<StateDto> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }
        public async Task<int> CreateAsync(State state)
        {
            return await _repository.CreateAsync(state);

        }

       

        public Task UpdateAsync(State state)
        {
            return _repository.UpdateAsync(state);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }


        public async Task<StateDto> GetByName(string name)
        {
            return await _repository.GetByNameAsync(name);
        }

        public async Task<bool> NameExistsAsync(string StateName, int excludeId)
        {
            return await _repository.NameExistsAsync(StateName, excludeId);
        }
    }
}
