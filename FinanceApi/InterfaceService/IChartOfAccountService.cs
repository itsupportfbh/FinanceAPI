using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IChartOfAccountService
    {
        Task<IEnumerable<ChartOfAccountDTO>> GetAllAsync();
        Task<ChartOfAccountDTO?> GetById(int id); // nullable
        Task<int> CreateAsync(ChartOfAccount entity);
        Task UpdateAsync(ChartOfAccount entity);
        Task DeleteChartOfAccount(int id);
    }

}
