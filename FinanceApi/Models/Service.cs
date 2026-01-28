namespace FinanceApi.Models
{
    public class Service : BaseEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int Charge { get; set; }
        public int Tax { get; set; }
        public string? Description { get; set; }
    }
}
