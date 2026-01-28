using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IMeetingService
    {
        Task<List<Meeting>> GetAllAsync();
        Task<Meeting?> GetByIdAsync(int id);
        Task<Meeting> CreateAsync(Meeting meeting);
        Task<Meeting?> UpdateAsync(int id, Meeting meeting);
        Task<bool> DeleteAsync(int id);
    }
}
