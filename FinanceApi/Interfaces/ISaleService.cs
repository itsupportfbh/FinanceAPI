using FinanceApi.Models;

namespace FinanceApi.Services
{
    public interface ISaleService
    {
        Task<List<SalesDto>> GetAllAsync();
        Task<SalesDto?> GetByIdAsync(int id);
        Task<Sales> CreateAsync(Sales sale);
        Task<Sales?> UpdateAsync(int id, Sales sale);
        Task<bool> DeleteAsync(int id);
    }
}

