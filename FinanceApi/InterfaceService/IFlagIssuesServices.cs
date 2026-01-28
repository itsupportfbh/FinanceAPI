using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IflagIssuesServices
    {
        Task<IEnumerable<FlagIssuesDTO>> GetAllAsync();
        Task<int> CreateAsync(FlagIssues flagIssuesDTO);
        Task<FlagIssuesDTO> GetById(long id);
       Task UpdateAsync(FlagIssues flagIssuesDTO);

        Task DeleteAsync(int id);

        Task<FlagIssuesDTO> GetByName(string name);
        Task<bool> NameExistsAsync(string Name, long excludeId);
    }
}
