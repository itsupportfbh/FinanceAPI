namespace FinanceApi.ModelDTO
{
    public class ItemSetItemDTO
    {
        public long Id { get; set; }
        public long ItemSetId { get; set; }
        public long ItemId { get; set; }
        public string UomName { get; set; }
        public string CreatedBy { get; set; } = string.Empty;

        public DateTime CreatedDate { get; set; }

        public bool IsActive { get; set; } = true;

    }
}
