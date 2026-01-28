using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    public class State : BaseEntity
    {
        public int Id { get; set; }
        public string StateName { get; set; }
        public int CountryId { get; set; }

        [ForeignKey("CountryId")]
        public Country? Country { get; set; }
    }

    public class StateDto
    {
        public int Id { get; set; }
        public int StateId { get; set; }
        public string StateName { get; set; }
        public int CountryId { get; set; }
        public string CountryName { get; set; }

        public string? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; }
    }
}
