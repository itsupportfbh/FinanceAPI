namespace FinanceApi.Models
{
    public class Recurring : BaseEntity
    {
        public int Id { get; set; }
        public string RecurringName { get; set; }
        public string CreatedBy { get; set; }

        public DateTime? CreatedDate { get; set; }

        public string UpdatedBy { get; set; }

        public DateTime? UpdatedDate { get; set; }

        public bool? IsActive { get; set; }
    }
}
