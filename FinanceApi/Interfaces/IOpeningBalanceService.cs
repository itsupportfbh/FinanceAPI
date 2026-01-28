using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IOpeningBalanceService
    {
        Task<IEnumerable<OpeningBalanceDto>> GetAllAsync();

        Task<OpeningBalanceDto> GetById(int id);
        Task<int> CreateAsync(OpeningBalance OpeningBalance);
        Task UpdateAsync(OpeningBalance OpeningBalance);

        Task DeleteAsync(int id);
    }
}
