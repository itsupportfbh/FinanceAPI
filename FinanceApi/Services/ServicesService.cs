using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class ServicesService : IServicesService
    {
        private readonly IServicesRepository _repository;

        public ServicesService(IServicesRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<ServiceDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<ServiceDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }
        public async Task<int> CreateAsync(Service service)
        {
            return await _repository.CreateAsync(service);

        }



        public Task UpdateAsync(Service service)
        {
            return _repository.UpdateAsync(service);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
