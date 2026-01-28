namespace FinanceApi.ModelDTO
{
    public class PeriodOptionDto
    {
        public int Id { get; set; }
        public string Label { get; set; } = "";
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public bool IsLocked { get; set; }
    }

    public class PeriodStatusDto
    {
        public int PeriodId { get; set; }
        public string PeriodLabel { get; set; } = "";
        public DateTime PeriodEndDate { get; set; }
        public bool IsLocked { get; set; }
    }

    public class FxRevalRequestDto
    {
        public int PeriodId { get; set; }
        public DateTime FxDate { get; set; }
    }
    public class AccountingPeriod
    {
        public int Id { get; set; }

        public string PeriodCode { get; set; } = string.Empty;  // e.g. 202603
        public string PeriodName { get; set; } = string.Empty;  // e.g. "Mar 2026"

        public DateTime StartDate { get; set; }                 // table: StartDate
        public DateTime EndDate { get; set; }                   // table: EndDate

        public bool IsLocked { get; set; }

        public DateTime? LockDate { get; set; }
        public int? LockedBy { get; set; }

        public bool IsActive { get; set; }
        public int? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public int? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
    }
}
