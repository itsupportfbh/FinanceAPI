using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IPurchaseRepository
    {
        Task<List<PurchasesDto>> GetAllAsync();
        Task<PurchasesDto?> GetByIdAsync(int id);
        Task<Purchases> CreateAsync(Purchases purchases);
        Task<Purchases?> UpdateAsync(int id, Purchases purchases);
        Task<bool> DeleteAsync(int id);
    }
}
