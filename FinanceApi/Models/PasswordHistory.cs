namespace FinanceApi.Models
{
    public class PasswordHistory
    {
        public int Id { get; set; }
        public int UserId { get; set; }
        public string PasswordHash { get; set; } = string.Empty;
        public DateTime ChangedAtUtc { get; set; } = DateTime.UtcNow;
        public User User { get; set; } = null!;
    }
}
