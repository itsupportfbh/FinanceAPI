namespace FinanceApi.Models
{
    public class MeetingAgendaDecision : BaseEntity
    {
        public int Id { get; set; }

        public int MeetingId { get; set; }

        public string Description { get; set; }

        public DateTime? DueDate { get; set; }

        public string? AssignedTo { get; set; }

        public string? DecisionNumber { get; set; }

        public string? RelatedAgendaItemTitle { get; set; }

        public string ResolutionStatus { get; set; }
    }
}
    