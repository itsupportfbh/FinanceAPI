namespace FinanceApi.ModelDTO
{
    public class CostingMethodDTO
    {
        public long ID { get; set; }
        public string CostingName { get; set; }
        public string description { get; set; }
        public long CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }

        public long UpdatedBy { get; set; }
        public DateTime UpdatedDate { get; set; }
        public bool IsActive { get; set; } = true;
    }
}
