using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IRecurringRepository
    {
        Task<IEnumerable<Recurring>> GetAllAsync();
        Task<Recurring> GetByIdAsync(int id);
        Task<int> CreateAsync(Recurring recurring);
        Task UpdateAsync(Recurring recurring);
        Task DeactivateAsync(int id);
        Task<RecurringDTO> GetByNameAsync(string name);
    }
}
