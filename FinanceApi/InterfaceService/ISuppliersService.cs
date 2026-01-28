using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface ISuppliersService
    {
        Task<IEnumerable<SuppliersDTO>> GetAllAsync();
        Task<int> CreateAsync(Suppliers supplier);
        Task<SuppliersDTO> GetById(int id);
        Task UpdateAsync(Suppliers supplier);
        Task DeleteLicense(int id);
    }
}
