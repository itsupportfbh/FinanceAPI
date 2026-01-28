using FinanceApi.ModelDTO;

namespace FinanceApi.Models
{
    public class ManualJournalDto
    {
        public int Id { get; set; }
        public string? JournalNo { get; set; }
        public DateTime JournalDate { get; set; }
        public string Description { get; set; } = string.Empty;

        public bool IsRecurring { get; set; }
        public string? RecurringFrequency { get; set; }
        public int? RecurringInterval { get; set; }
        public DateTime? RecurringStartDate { get; set; }
        public string? RecurringEndType { get; set; }
        public DateTime? RecurringEndDate { get; set; }
        public int? RecurringCount { get; set; }
        public int ProcessedCount { get; set; }
        public DateTime? NextRunDate { get; set; }

        public bool IsPosted { get; set; }

        public string? Timezone { get; set; }

        public IEnumerable<ManualJournalLineDto> Lines { get; set; } = Array.Empty<ManualJournalLineDto>();

    }

}
