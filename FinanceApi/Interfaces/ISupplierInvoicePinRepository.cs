using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ISupplierInvoicePinRepository
    {
        Task<IEnumerable<SupplierInvoicePin>> GetAllAsync();
        Task<SupplierInvoicePinDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(SupplierInvoicePin pin);
        Task UpdateAsync(SupplierInvoicePin pin);
        Task DeactivateAsync(int id);
        Task<ThreeWayMatchDTO?> GetThreeWayMatchAsync(int pinId);
        Task FlagForReviewAsync(int pinId, string userName);
        Task PostToApAsync(int pinId, string userName);
    }
}
