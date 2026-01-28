using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IChartOfAccountRepository
    {
        Task<IEnumerable<ChartOfAccountDTO>> GetAllAsync();
        Task<ChartOfAccountDTO?> GetByIdAsync(int id); // nullable
        Task<int> CreateAsync(ChartOfAccount entity);
        Task UpdateAsync(ChartOfAccount entity);
        Task DeactivateAsync(int id);
    }
}
