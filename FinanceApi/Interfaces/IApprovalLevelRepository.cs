using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IApprovalLevelRepository
    {
        Task<IEnumerable<ApprovalLevelDTO>> GetAllAsync();
        Task<ApprovalLevelDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(ApprovalLevel approvalLevel);
        Task UpdateAsync(ApprovalLevel approvalLevel);
        Task DeactivateAsync(int id);
        Task<ApprovalLevelDTO> GetByNameAsync(string name);
        Task<bool> NameExistsAsync(string name, int excludeId);
    }
}
