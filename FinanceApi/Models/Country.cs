namespace FinanceApi.Models
{
    public class Country : BaseEntity
    {
        public int Id { get; set; }
        public string CountryName { get; set; }
        public decimal GSTPercentage { get; set; }
        public string CreatedBy { get; set; }

        public DateTime? CreatedDate { get; set; }

        public string UpdatedBy { get; set; }

        public DateTime? UpdatedDate { get; set; }

        public bool? IsActive { get; set; }
    }
}
