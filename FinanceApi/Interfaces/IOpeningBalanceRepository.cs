using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IOpeningBalanceRepository
    {
        Task<IEnumerable<OpeningBalanceDto>> GetAllAsync();
        Task<OpeningBalanceDto> GetByIdAsync(int id);
        Task<int> CreateAsync(OpeningBalance OpeningBalance);
        Task UpdateAsync(OpeningBalance OpeningBalance);
        Task DeactivateAsync(int id);
    }
}
