using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IPurchaseRequestTempService
    {
        Task<int> CreateAsync(PurchaseRequestTempDto dto);
        Task UpdateAsync(PurchaseRequestTempDto dto);
        Task<PurchaseRequestTemp> GetByIdAsync(int id);
        Task<IEnumerable<PurchaseRequestTempDto>> ListAsync(int? departmentId = null);
        Task DeleteAsync(int id, string userId);
        Task<int> PromoteAsync(int tempId, string userId);
    }
}
