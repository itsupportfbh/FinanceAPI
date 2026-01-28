using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface ISupplierInvoicePinService
    {
        Task<IEnumerable<SupplierInvoicePin>> GetAllAsync();
        Task<SupplierInvoicePinDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(SupplierInvoicePin pin);
        Task UpdateAsync(SupplierInvoicePin pin);
        Task DeleteAsync(int id);
        Task<ThreeWayMatchDTO?> GetThreeWayMatchAsync(int pinId);
        Task FlagForReviewAsync(int pinId, string userName);
        Task PostToApAsync(int pinId, string userName);
    }
}
