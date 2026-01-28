using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IIncomeRepository
    {
        Task<IEnumerable<Income>> GetAllAsync();
        Task<Income> GetByIdAsync(int id);
        Task<int> CreateAsync(Income income);
        Task UpdateAsync(Income income);
        Task DeactivateAsync(int id);
    }
}
