using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IPurchaseOrderTempService
    {
        Task<IEnumerable<PurchaseOrderTempDto>> GetDraftsAsync(string? createdBy = null);
        Task<PurchaseOrderTemp?> GetByIdAsync(int id);
        Task<int> CreateAsync(PurchaseOrderTemp draft);
        Task UpdateAsync(PurchaseOrderTemp draft);
        Task DeleteAsync(int id);
        Task<int> PromoteAsync(int draftId, string promotedBy);
    }
}
