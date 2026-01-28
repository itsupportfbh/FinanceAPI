using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FinanceApi.Services
{
    public class JournalService : IJournalService
    {
        private readonly IJournalRepository _journalRepository;

        public JournalService(IJournalRepository journalRepository)
        {
            _journalRepository = journalRepository;
        }

        public Task<IEnumerable<JournalsDTO>> GetAllAsync()
            => _journalRepository.GetAllAsync();

        public Task<IEnumerable<ManualJournalDto>> GetAllRecurringDetails()
            => _journalRepository.GetAllRecurringDetails();

        public Task<ManualJournalDto?> GetById(int id)
            => _journalRepository.GetByIdAsync(id);

        public Task<int> CreateAsync(ManualJournalCreateDto dto)
            => _journalRepository.CreateAsync(dto);

        public async Task<int> ProcessRecurringAsync(DateTime nowLocal, string timezone)
        {
            var tz = TimeZoneInfo.FindSystemTimeZoneById(timezone);
            var nowUtc = TimeZoneInfo.ConvertTimeToUtc(nowLocal, tz);
            return await _journalRepository.ProcessRecurringAsync(nowUtc);
        }

        public Task<int> MarkAsPostedAsync(IEnumerable<int> ids)
            => _journalRepository.MarkAsPostedAsync(ids);
    }
}
