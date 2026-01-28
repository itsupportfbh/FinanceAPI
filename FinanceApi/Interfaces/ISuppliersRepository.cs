using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ISuppliersRepository
    {
        Task<IEnumerable<SuppliersDTO>> GetAllAsync();
        Task<SuppliersDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(Suppliers supplier);
        Task UpdateAsync(Suppliers supplierDto);
        Task DeactivateAsync(int id);
    }
}
