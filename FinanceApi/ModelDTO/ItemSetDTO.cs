namespace FinanceApi.ModelDTO
{
    public class ItemSetDTO
    {
        public long Id { get; set; }
        public string SetName { get; set; }
        public string CreatedBy { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime? UpdatedDate { get; set; }

        public bool IsActive { get; set; } = true;


        public List<ItemSetItemDTO> Items { get; set; } = new();
    }
}
