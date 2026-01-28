namespace FinanceApi.Models
{
    public class PasswordResetToken
    {
        public int Id { get; set; }

        public int UserId { get; set; }

        public string Token { get; set; }

        public DateTime Expiration { get; set; }

        public bool IsUsed { get; set; } = false;

        public virtual User User { get; set; }
    }
}
