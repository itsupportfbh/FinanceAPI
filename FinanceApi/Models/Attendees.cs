namespace FinanceApi.Models
{
    public class Attendees : BaseEntity
    {
        public int Id { get; set; }

        public int MeetingId { get; set; }
        public string Attendee { get; set; }

        public string AttendeeType { get; set; }

        public string AttendeeStatus { get; set; }
    }
}
