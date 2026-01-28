using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class CityService : ICityService
    {
        private readonly ICityRepository _repository;

        public CityService(ICityRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<CityDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<CityDto> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }
        public async Task<int> CreateAsync(City city)
        {
            return await _repository.CreateAsync(city);

        }



        public Task UpdateAsync(City city)
        {
            return _repository.UpdateAsync(city);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }

     


        public async Task<IEnumerable<CityDto>> GetStateWithCountryId(int id)
        {
            return await _repository.GetStateWithCountryId(id);
        }

        public async Task<IEnumerable<CityDto>> GetCityWithStateId(int id)
        {
            return await _repository.GetCityWithStateId(id);
        }
        public async Task<CityDto> GetByNameInCountryAsync(string name, int countryId)
        {
            return await _repository.GetByNameInCountryAsync(name,countryId);
        }

        public async Task<bool> NameExistsAsync(string name, int countryId, int excludeId)
        {
            return await _repository.NameExistsAsync(name, countryId, excludeId);
        }
    }
}
