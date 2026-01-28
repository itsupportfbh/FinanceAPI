namespace FinanceApi.ModelDTO
{
    public class ItemTypeDTO
    {
        public int Id { get; set; }

        public string ItemTypeName { get; set; }

        public string Description { get; set; }

        public string CreatedBy { get; set; }

        public DateTime? CreatedDate { get; set; }

        public string UpdatedBy { get; set; }

        public DateTime? UpdatedDate { get; set; }

        public bool? IsActive { get; set; }
    }
}
