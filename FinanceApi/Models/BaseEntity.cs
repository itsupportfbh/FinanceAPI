namespace FinanceApi.Models
{
    public abstract class BaseEntity
    {
        public string? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; } = DateTime.UtcNow;

        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }

        public bool IsActive { get; set; } = true;
    }
}

