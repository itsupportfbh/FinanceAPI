using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;


namespace FinanceApi.Services
{
    public class CountryService : ICountryService
    {
        private readonly ICountryRepository _repository; 

        public CountryService(ICountryRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<Country>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(Country country)
        {
            return await _repository.CreateAsync(country);

        }

        public async Task<Country> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(Country country)
        {
            return _repository.UpdateAsync(country);
        }


        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }

        public async Task<Country> GetByName(string name)
        {
            return await _repository.GetByNameAsync(name);
        }

        public async Task<bool> NameExistsAsync(string CountryName, int excludeId)
        {
            return await _repository.NameExistsAsync(CountryName, excludeId);
        }
    }
}
