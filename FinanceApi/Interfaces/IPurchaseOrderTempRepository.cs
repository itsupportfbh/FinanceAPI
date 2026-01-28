using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IPurchaseOrderTempRepository
    {
        Task<IEnumerable<PurchaseOrderTempDto>> GetDraftsAsync(string? createdBy = null);
        Task<PurchaseOrderTemp?> GetByIdAsync(int id);
        Task<int> CreateAsync(PurchaseOrderTemp draft);
        Task UpdateAsync(PurchaseOrderTemp draft);
        Task DeleteAsync(int id);                // soft delete

        // Promotion (returns new PO Id)
        Task<int> PromoteToPoAsync(int draftId, string promotedBy);
    }
}
