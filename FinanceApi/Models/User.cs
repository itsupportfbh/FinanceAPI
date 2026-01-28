namespace FinanceApi.Models
{
    public class User : BaseEntity
    {
        public int Id { get; set; }
        public string Username { get; set; }
        public string PasswordHash { get; set; }
        public string Email { get; set; }

        public int DepartmentId { get; set; }   
        public ICollection<PasswordResetToken> PasswordResetTokens { get; set; }
    }

    public class UserDto
    {
        public string Username { get; set; }
        public string? Password { get; set; } // only used temporarily for hashing
        public string? Email { get; set; }
        public string? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool? IsActive { get; set; }
        public List<int> ApprovalLevelIds { get; set; } = new();
        public int DepartmentId { get; set; }
    }

    public class LoginResponseDto
    {
        public int UserId { get; set; }
        public string Username { get; set; } = "";
        public string Token { get; set; } = "";
        public string Email { get; set; } = "";

        // ✅ Teams (UserRole)
        public List<string> Teams { get; set; } = new();

        // ✅ Approval Roles (ApprovalLevel)
        public List<int> ApprovalLevelIds { get; set; } = new();
        public List<string> ApprovalLevelNames { get; set; } = new();
    }




    public class UserViewDto
    {
        public int Id { get; set; }
        public string Username { get; set; } = "";
        public string Email { get; set; } = "";
        public bool IsActive { get; set; }
        public int DepartmentId { get; set; }
        public List<int> ApprovalLevelIds { get; set; } = new();
        public List<string> ApprovalLevelNames { get; set; } = new();
        public List<string> Teams { get; set; } = new();
    }

}
