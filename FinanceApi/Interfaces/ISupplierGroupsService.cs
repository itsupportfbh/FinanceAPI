using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ISupplierGroupsService
    {
        Task<IEnumerable<SupplierGroupDTO>> GetAllAsync();
        Task<SupplierGroupDTO> GetById(int id);
        Task<int> CreateAsync(SupplierGroups supplierGroups);
        Task UpdateAsync(SupplierGroups supplierGroups);
        Task DeleteAsync(int id);
    }
}
