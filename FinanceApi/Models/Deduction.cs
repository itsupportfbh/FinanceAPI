namespace FinanceApi.Models
{
    public class Deduction : BaseEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string? Description { get; set; }
    }
}
