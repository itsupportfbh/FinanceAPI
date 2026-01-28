using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IDeductionRepository
    {       
        Task<IEnumerable<Deduction>> GetAllAsync();
        Task<Deduction> GetByIdAsync(int id);
        Task<int> CreateAsync(Deduction deduction);
        Task UpdateAsync(Deduction deduction);
        Task DeactivateAsync(int id);
    }
}
