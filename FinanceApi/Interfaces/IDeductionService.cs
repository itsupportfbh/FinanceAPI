using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IDeductionService
    {     
        Task<IEnumerable<Deduction>> GetAllAsync();
        Task<Deduction> GetByIdAsync(int id);
        Task<int> CreateAsync(Deduction deduction);
        Task UpdateAsync(Deduction deduction);
        Task DeleteLicense(int id);
    }
}
