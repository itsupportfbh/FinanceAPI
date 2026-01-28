using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ICreditNoteRepository
    {
        Task<IEnumerable<CreditNoteDTO>> GetAllAsync();
        Task<CreditNoteDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(CreditNote cn);
        Task UpdateAsync(CreditNote cn);
        Task DeactivateAsync(int id, int updatedBy);

        // UI helper
        Task<IEnumerable<object>> GetDoLinesAsync(int doId, int? excludeCnId = null);
    }
}
