namespace FinanceApi.Models
{
    public class Meeting : BaseEntity
    {
        public int Id { get; set; }
        public string MeetingName { get; set; }

        public string MeetingType { get; set; }
        
        public DateTime StartDate { get; set; }

        public DateTime EndDate { get; set; }

        public string? Department { get; set; }        

        public string Location { get; set; }       

        public string? OrganisedBy { get; set; }

        public string? Reporter { get; set; }

        public List<Attendees> Attendees { get; set; } = new();

        public List<MeetingAgendaItems> MeetingAgendaItems { get; set; } = new();

        public List<MeetingAgendaDecision> MeetingAgendaDecisions { get; set; } = new();

    }
}
