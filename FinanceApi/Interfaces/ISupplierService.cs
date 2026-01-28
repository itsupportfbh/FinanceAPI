using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ISupplierService
    {
        Task<List<SupplierDto>> GetAllAsync();
        Task<SupplierDto?> GetByIdAsync(int id);
        Task<Supplier> CreateAsync(Supplier supplier);
        Task<Supplier?> UpdateAsync(int id, Supplier supplier);
        Task<bool> DeleteAsync(int id);
    }
}
