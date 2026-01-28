namespace FinanceApi.Models
{
    public class TaxCode : BaseEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int Rate { get; set; }
        public int TypeId { get; set; }
        public string CreatedBy { get; set; }

        public DateTime? CreatedDate { get; set; }

        public string UpdatedBy { get; set; }

        public DateTime? UpdatedDate { get; set; }

        public bool? IsActive { get; set; }
    }
}
