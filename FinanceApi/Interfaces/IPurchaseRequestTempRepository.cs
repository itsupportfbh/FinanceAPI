using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;

namespace FinanceApi.Interfaces
{
    public interface IPurchaseRequestTempRepository
    {
        Task<int> CreateAsync(PurchaseRequestTemp temp, IDbTransaction tx = null);
        Task UpdateAsync(PurchaseRequestTemp temp, IDbTransaction tx = null);
        Task<PurchaseRequestTemp> GetByIdAsync(int id);
        Task<IEnumerable<PurchaseRequestTempDto>> ListAsync(int? departmentId = null);
        Task DeleteAsync(int id, string userId); // soft delete: IsActive = 0

        // Promote draft -> real PurchaseRequest (returns new PR Id)
        Task<int> PromoteAsync(int tempId, string userId);
    }
}
