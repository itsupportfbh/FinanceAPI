using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class LocationService : ILocationService
    {
        private readonly ILocationRepository _repository;

        public LocationService(ILocationRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<LocationDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<IEnumerable<LocationDto>> GetAllLocationDetails()
        {
            return await _repository.GetAllLocationDetails();
        }

        public async Task<LocationDto> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }
        public async Task<int> CreateAsync(Location location)
        {
            return await _repository.CreateAsync(location);

        }



        public Task UpdateAsync(Location location)
        {
            return _repository.UpdateAsync(location);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }


        public async Task<IEnumerable<LocationDto>> GetLocationByCountryId(int id)
        {
            return await _repository.GetLocationbyCountryId(id);
        }
    }
}
