using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IIncomeService
    {     
        Task<IEnumerable<Income>> GetAllAsync();
        Task<Income> GetByIdAsync(int id);
        Task<int> CreateAsync(Income income);
        Task UpdateAsync(Income income);
        Task DeleteLicense(int id);
    }
}
