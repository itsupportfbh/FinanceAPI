using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class PurchaseOrderTempService : IPurchaseOrderTempService
    {
        private readonly IPurchaseOrderTempRepository _repo;

        public PurchaseOrderTempService(IPurchaseOrderTempRepository repo)
        {
            _repo = repo;
        }

        public Task<IEnumerable<PurchaseOrderTempDto>> GetDraftsAsync(string? createdBy = null)
            => _repo.GetDraftsAsync(createdBy);

        public Task<PurchaseOrderTemp?> GetByIdAsync(int id)
            => _repo.GetByIdAsync(id);

        public async Task<int> CreateAsync(PurchaseOrderTemp draft)
        {
            // Normalize for drafts (allow partials)
            draft.CreatedDate = DateTime.UtcNow;
            draft.IsActive = true;
            // Do NOT touch PRs here. That happens on Promote.
            return await _repo.CreateAsync(draft);
        }

        public async Task UpdateAsync(PurchaseOrderTemp draft)
        {
            // Only persistence; no PR side effects
            await _repo.UpdateAsync(draft);
        }

        public Task DeleteAsync(int id)
            => _repo.DeleteAsync(id);

        public Task<int> PromoteAsync(int draftId, string promotedBy)
            => _repo.PromoteToPoAsync(draftId, promotedBy);
    }

}
