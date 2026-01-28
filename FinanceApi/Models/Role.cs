namespace FinanceApi.Models
{
    public class Role
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
    public class UserRole
    {
        public int UserId { get; set; }
        public int RoleId { get; set; }

        public User User { get; set; }
        public Role Role { get; set; }
    }
}
