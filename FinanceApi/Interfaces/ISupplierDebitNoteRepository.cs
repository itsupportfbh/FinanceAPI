using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ISupplierDebitNoteRepository
    {
        Task<IEnumerable<SupplierDebitNoteDTO>> GetAllAsync();
        Task<SupplierDebitNoteDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(SupplierDebitNote model);
        Task UpdateAsync(SupplierDebitNote model);
        Task DeleteAsync(int id);
        Task<dynamic?> GetDebitNoteSourceAsync(int pinId);
        Task MarkDebitNoteAsync(int pinId, string userName);
    }
}
