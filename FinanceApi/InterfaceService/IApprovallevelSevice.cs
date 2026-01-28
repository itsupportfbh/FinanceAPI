using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IApprovallevelSevice
    {
        Task<IEnumerable<ApprovalLevelDTO>> GetAllAsync();
        Task<int> CreateAsync(ApprovalLevel approvalLevel);
        Task<ApprovalLevelDTO> GetById(int id);
        Task UpdateAsync(ApprovalLevel approvalLevel);
        Task DeleteLicense(int id);
        Task<ApprovalLevelDTO> GetByName(string name);
        Task<bool> NameExistsAsync(string name, int excludeId);
    }
}
