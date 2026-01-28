using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    public class Location : BaseEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int? CountryId { get; set; }
        public int? StateId { get; set; }
        public int? CityId { get; set; }
        public string ContactNumber { get; set; }
        public string? Latitude { get; set; }
        public string? Longitude { get; set; }
  

        // Optional navigation properties (not required, but helpful)

        [ForeignKey("CountryId")]
        public Country? Country { get; set; }

        [ForeignKey("StateId")]
        public State? State { get; set; }

        [ForeignKey("CityId")]
        public City? City { get; set; }
    }

    public class LocationDto
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int? CityId { get; set; }
        public string? CityName { get; set; }
        public int? StateId { get; set; }
        public string? StateName { get; set; }
        public int? CountryId { get; set; }
        public string? CountryName { get; set; }
        public string ContactNumber { get; set; }
        public string? Latitude { get; set; }
        public string? Longitude { get; set; }


        public string? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; }
    }
}
