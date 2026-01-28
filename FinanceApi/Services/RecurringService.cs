using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class RecurringService : IRecurringService
    {
        private readonly IRecurringRepository _repository;

        public RecurringService(IRecurringRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<Recurring>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(Recurring recurring)
        {
            return await _repository.CreateAsync(recurring);

        }

        public async Task<Recurring> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(Recurring recurring)
        {
            return _repository.UpdateAsync(recurring);
        }


        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }


        public async Task<RecurringDTO> GetByName(string name)
        {
            return await _repository.GetByNameAsync(name);
        }
    }
}
