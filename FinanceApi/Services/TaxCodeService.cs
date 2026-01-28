using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class TaxCodeService : ITaxCodeService
    {
        private readonly ITaxCodeRepository _repository;

        public TaxCodeService(ITaxCodeRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<TaxCode>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(TaxCode taxCode)
        {
            return await _repository.CreateAsync(taxCode);

        }

        public async Task<TaxCode> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(TaxCode taxCode)
        {
            return _repository.UpdateAsync(taxCode);
        }


        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }

        public async Task<TaxCodeDTO> GetByName(string name)
        {
            return await _repository.GetByNameAsync(name);
        }
    }
}
