using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IRecurringService
    {
        Task<IEnumerable<Recurring>> GetAllAsync();
        Task<Recurring> GetById(int id);
        Task<int> CreateAsync(Recurring recurring);
        Task UpdateAsync(Recurring recurring);
        Task DeleteLicense(int id);

        Task<RecurringDTO> GetByName(string name);
    }
}
