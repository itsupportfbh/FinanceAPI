using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface ISupplierDebitNoteService
    {
        Task<IEnumerable<SupplierDebitNoteDTO>> GetAllAsync();
        Task<SupplierDebitNoteDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(SupplierDebitNote model);
        Task UpdateAsync(SupplierDebitNote model);
        Task DeleteAsync(int id);
        // InterfaceService/ISupplierInvoicePinService.cs
        Task<dynamic?> GetDebitNoteSourceAsync(int pinId);
        Task MarkDebitNoteAsync(int pinId, string userName);

    }
}
