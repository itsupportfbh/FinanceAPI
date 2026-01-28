using FinanceApi.Interfaces;
using FinanceApi.Models;


namespace FinanceApi.Services
{
    public class MeetingService : IMeetingService
    {
        private readonly IMeetingRepository _repository;

        public MeetingService(IMeetingRepository repository)
        {
            _repository = repository;
        }

        public async Task<List<Meeting>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<Meeting?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<Meeting> CreateAsync(Meeting meeting)
        {
            // Example validation logic you might add later
            if (meeting.Attendees == null || !meeting.Attendees.Any())
                throw new ArgumentException("At least one Attendee is required.");

            if (meeting.MeetingAgendaItems == null || !meeting.MeetingAgendaItems.Any())
                throw new ArgumentException("Meeting Agenda Items is required.");

            if (meeting.MeetingAgendaDecisions == null || !meeting.MeetingAgendaDecisions.Any())
                throw new ArgumentException("Meeting Agenda Decisions is required.");

            return await _repository.CreateAsync(meeting);
        }

        public async Task<Meeting?> UpdateAsync(int id, Meeting meeting)
        {
            return await _repository.UpdateAsync(id, meeting);
        }

        public async Task<bool> DeleteAsync(int id)
        {
            return await _repository.DeleteAsync(id);
        }
    }
}
