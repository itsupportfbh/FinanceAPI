using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class SuppliersService : ISuppliersService
    {
        private readonly ISuppliersRepository _repository;

        public SuppliersService(ISuppliersRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<SuppliersDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(Suppliers supplier)
        {
            return await _repository.CreateAsync(supplier);

        }

        public async Task<SuppliersDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(Suppliers supplier)
        {
            return _repository.UpdateAsync(supplier);
        }


        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }

}

