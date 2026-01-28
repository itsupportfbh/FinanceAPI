using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface ICreditNoteService
    {
        Task<IEnumerable<CreditNoteDTO>> GetAllAsync();
        Task<CreditNoteDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(CreditNote cn);
        Task UpdateAsync(CreditNote cn);
        Task DeactivateAsync(int id, int updatedBy);

        // DO pool (UI)
        Task<IEnumerable<object>> GetDoLinesAsync(int doId, int? excludeCnId = null);
    }
}
