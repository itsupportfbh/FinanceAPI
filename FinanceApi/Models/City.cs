using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;

namespace FinanceApi.Models
{
    public class City : BaseEntity
    {
        public int Id { get; set; }
        public string CityName { get; set; }
        public int StateId { get; set; }

        [ForeignKey("StateId")]
        public State? State { get; set; }
        [JsonPropertyName("countryId")]
        public int CountryId { get; set; }

        [ForeignKey("CountryId")]
        public Country? Country { get; set; }
    }

    public class CityDto
    {
        public int Id { get; set; }
        public int CityId { get; set; }
        public string CityName { get; set; }
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
