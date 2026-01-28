using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IJournalService
    {
        Task<IEnumerable<JournalsDTO>> GetAllAsync();
        Task<IEnumerable<ManualJournalDto>> GetAllRecurringDetails();
        Task<int> CreateAsync(ManualJournalCreateDto dto);
        Task<ManualJournalDto?> GetById(int id);

        Task<int> ProcessRecurringAsync(DateTime nowLocal, string timezone);

        Task<int> MarkAsPostedAsync(IEnumerable<int> ids);
    }
}
