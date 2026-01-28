using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ISupplierGroupsRepository
    {
        Task<IEnumerable<SupplierGroupDTO>> GetAllAsync();
        Task<SupplierGroupDTO> GetByIdAsync(long id);
        Task<int> CreateAsync(SupplierGroups supplierGroups);
        Task UpdateAsync(SupplierGroups supplierGroups);
        Task DeactivateAsync(int id);
    }
}
