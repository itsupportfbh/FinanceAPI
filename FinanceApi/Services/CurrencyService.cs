using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class CurrencyService : ICurrencyService
    {
        private readonly ICurrencyRepository _repository;

        public CurrencyService(ICurrencyRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<CurrencyDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(Currency currencyDTO)
        {
            return await _repository.CreateAsync(currencyDTO);

        }

        public async Task<CurrencyDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(Currency currencyDTO)
        {
            return _repository.UpdateAsync(currencyDTO);
        }


        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }

        public async Task<CurrencyDTO> GetByName(string name)
        {
            return await _repository.GetByNameAsync(name);
        }

        public async Task<bool> NameExistsAsync(string currencyName, int excludeId)
        {
            return await _repository.NameExistsAsync(currencyName, excludeId);
        }
    }

}
