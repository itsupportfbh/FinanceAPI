using FinanceApi.Models;
using FinanceApi.Data;
using Microsoft.EntityFrameworkCore;
using FinanceApi.Interfaces;

namespace FinanceApi.Repositories
{
    public class MeetingRepository : IMeetingRepository
    {
        private readonly ApplicationDbContext _context;

        public MeetingRepository(ApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<List<Meeting>> GetAllAsync()
        {
            return await _context.Meetings.Include(s => s.Attendees)
            .Include(s => s.MeetingAgendaItems).Include(s => s.MeetingAgendaDecisions).Where(c => c.IsActive).OrderBy(c => c.Id).ToListAsync();
        }

        public async Task<Meeting?> GetByIdAsync(int id)
        {
            return await _context.Meetings.Include(s => s.Attendees)
            .Include(s => s.MeetingAgendaItems).Include(s => s.MeetingAgendaDecisions).Where(c => c.IsActive).FirstOrDefaultAsync(s => s.Id == id);
        }

        public async Task<Meeting> CreateAsync(Meeting meeting)
        {

            try
            {
                // Optional: Defensive null check
                if (meeting == null)
                    throw new ArgumentNullException(nameof(meeting));
                meeting.CreatedBy = "System";
                meeting.CreatedDate = DateTime.UtcNow;
                meeting.IsActive = true;
                _context.Meetings.Add(meeting);
                await _context.SaveChangesAsync();
                return meeting;
            }
            catch (DbUpdateException ex)
            {
                // Logs more useful detail from inner exception (e.g., PostgreSQL error)
                var errorMessage = ex.InnerException?.Message ?? ex.Message;
                Console.WriteLine($"EF Save Error: {errorMessage}");
                throw; // Consider wrapping this in a custom exception for higher-level handling
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unexpected error: {ex.Message}");
                throw;
            }
        }

        public async Task<Meeting?> UpdateAsync(int id, Meeting updatedMeeting)
        {
            try
            {
                var existingMeeting = await _context.Meetings.Include(s => s.Attendees)
              .Include(s => s.MeetingAgendaItems).Include(s => s.MeetingAgendaDecisions).FirstOrDefaultAsync(s => s.Id == id);
                if (existingMeeting == null) return null;

                // Manually update only scalar properties (excluding Id)
                existingMeeting.MeetingName = updatedMeeting.MeetingName;
                existingMeeting.MeetingType = updatedMeeting.MeetingType;
                existingMeeting.StartDate = updatedMeeting.StartDate;
                existingMeeting.EndDate = updatedMeeting.EndDate;
                existingMeeting.Department = updatedMeeting.Department;
                existingMeeting.Location = updatedMeeting.Location;
                existingMeeting.OrganisedBy = updatedMeeting.OrganisedBy;
                existingMeeting.Reporter = updatedMeeting.Reporter;
                existingMeeting.UpdatedBy = "System";
                existingMeeting.UpdatedDate = DateTime.UtcNow;



                existingMeeting.Attendees.Clear();
                foreach (var item in updatedMeeting.Attendees)
                {
                    existingMeeting.Attendees.Add(item);
                }
                existingMeeting.MeetingAgendaItems.Clear();
                foreach (var item in updatedMeeting.MeetingAgendaItems)
                {
                    existingMeeting.MeetingAgendaItems.Add(item);
                }
                existingMeeting.MeetingAgendaDecisions.Clear();
                foreach (var item in updatedMeeting.MeetingAgendaDecisions)
                {
                    existingMeeting.MeetingAgendaDecisions.Add(item);
                }

                await _context.SaveChangesAsync();
                return existingMeeting;
            }
             catch (DbUpdateException ex)
            {
                // Logs more useful detail from inner exception (e.g., PostgreSQL error)
                var errorMessage = ex.InnerException?.Message ?? ex.Message;
                Console.WriteLine($"EF Save Error: {errorMessage}");
                throw; // Consider wrapping this in a custom exception for higher-level handling
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unexpected error: {ex.Message}");
                throw;
            }
        }


        public async Task<bool> DeleteAsync(int id)
        {
            var meeting = await _context.Meetings.Include(s => s.Attendees)
            .Include(s => s.MeetingAgendaItems).Include(s => s.MeetingAgendaDecisions).FirstOrDefaultAsync(s => s.Id == id);

            if (meeting == null) return false;

            meeting.IsActive = false;
            await _context.SaveChangesAsync();
            return true;
        }
    }
}
