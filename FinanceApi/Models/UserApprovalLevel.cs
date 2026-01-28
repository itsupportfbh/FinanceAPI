namespace FinanceApi.Models
{
    public class UserApprovalLevel
    {
        public int UserId { get; set; }
        public User User { get; set; } = null!;

        public int ApprovalLevelId { get; set; }
        public ApprovalLevel ApprovalLevel { get; set; } = null!;

        public string? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; } = DateTime.UtcNow;
    }
}
