using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IJournalRepository
    {
        Task<IEnumerable<JournalsDTO>> GetAllAsync();
        Task<IEnumerable<ManualJournalDto>> GetAllRecurringDetails();
        Task<int> CreateAsync(ManualJournalCreateDto dto);

        Task<ManualJournalDto?> GetByIdAsync(int id);

        // expects UTC time
        Task<int> ProcessRecurringAsync(DateTime processUtc);

        Task<int> MarkAsPostedAsync(IEnumerable<int> ids);
    }
}
