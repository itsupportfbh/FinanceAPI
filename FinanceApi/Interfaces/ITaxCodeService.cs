using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ITaxCodeService
    {
        Task<IEnumerable<TaxCode>> GetAllAsync();
        Task<TaxCode> GetById(int id);
        Task<int> CreateAsync(TaxCode taxCode);
        Task UpdateAsync(TaxCode taxCode);
        Task DeleteLicense(int id);

        Task<TaxCodeDTO> GetByName(string name);
    }
}
