using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ITaxCodeRepository
    {
        Task<IEnumerable<TaxCode>> GetAllAsync();
        Task<TaxCode> GetByIdAsync(int id);
        Task<int> CreateAsync(TaxCode taxCode);
        Task UpdateAsync(TaxCode taxCode);
        Task DeactivateAsync(int id);

        Task<TaxCodeDTO> GetByNameAsync(string name);
    }
}
