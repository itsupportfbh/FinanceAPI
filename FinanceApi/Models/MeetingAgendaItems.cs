namespace FinanceApi.Models
{
    public class MeetingAgendaItems : BaseEntity
    {
        public int Id { get; set; }

        public int MeetingId { get; set; }
        public string Title { get; set; }

        public string? Description { get; set; }

        public string ItemTypeName { get; set; }

        public string? RequestedBy { get; set; }

        public string SequenceNo { get; set; }
    }
}
