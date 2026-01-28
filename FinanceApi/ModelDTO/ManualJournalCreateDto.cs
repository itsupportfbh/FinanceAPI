// ModelDTO/ManualJournalCreateDto.cs
using System;

namespace FinanceApi.ModelDTO
{
    public class ManualJournalCreateDto
    {
        // Incoming from UI (string dates)
        public string JournalDate { get; set; } = string.Empty;
        public string? RecurringStartDate { get; set; }
        public string? RecurringEndDate { get; set; }

        // Converted to UTC in controller
        public DateTime JournalDateUtc { get; set; }
        public DateTime? RecurringStartDateUtc { get; set; }
        public DateTime? RecurringEndDateUtc { get; set; }

        public string Description { get; set; } = string.Empty;

        public bool IsRecurring { get; set; }
        public string? RecurringFrequency { get; set; }     // Daily / Weekly / Monthly / ...
        public int? RecurringInterval { get; set; }         // every N
        public string? RecurringEndType { get; set; }       // NoEnd / EndByDate / EndByCount
        public int? RecurringCount { get; set; }

        public string Timezone { get; set; } = "Asia/Kolkata";

        public int CreatedBy { get; set; }

        // Lines from UI
        public List<ManualJournalLineCreateDto> Lines { get; set; } = new();
    }
}
