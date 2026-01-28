using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IFlagIssuesRepository
    {
        Task<IEnumerable<FlagIssuesDTO>> GetAllAsync();
        Task<FlagIssuesDTO> GetByIdAsync(long id);

        Task<int> CreateAsync(FlagIssues flagIssuesDTO);

        Task UpdateAsync(FlagIssues flagIssuesDTO);

        Task DeactivateAsync(int id);

        Task<bool> NameExistsAsync(string FlagIssuesNames, long excludeId);
        Task<FlagIssuesDTO> GetByNameAsync(string name);
    }
}
