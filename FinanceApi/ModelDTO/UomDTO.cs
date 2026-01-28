namespace FinanceApi.ModelDTO
{
    public class UomDTO
    {
        public int Id { get; set; }
        public string Name { get; set; } = default!;
        public string? Description { get; set; }
        public string? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; }
    }
}
