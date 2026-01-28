using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    public class Supplier : BaseEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string? ContactName { get; set; }
        public string? ContactTitle { get; set; }
        public int CountryId { get; set; }
        public int StateId { get; set; }
        public int CityId { get; set; }
        public int SupplierGroupId { get; set; }
        public string? Address { get; set; }
        public int RegionId { get; set; }
        public string? PostalCode { get; set; }
        public string Phone { get; set; }
        public string? Website { get; set; }
        public string? Fax { get; set; }
        public string? Email { get; set; }

        // Optional navigation properties (not required, but helpful)

        [ForeignKey("CountryId")]
        public Country? Country { get; set; }

        [ForeignKey("StateId")]
        public State? State { get; set; }

        [ForeignKey("CityId")]
        public City? City { get; set; }

        [ForeignKey("SupplierGroupId")]
        public SupplierGroups? SupplierGroups { get; set; }

        [ForeignKey("RegionId")]
        public Region? Region { get; set; }

    }

    public class SupplierDto
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string? ContactName { get; set; }
        public string? ContactTitle { get; set; }
        public int CityId { get; set; }
        public string? CityName { get; set; }
        public int StateId { get; set; }
        public string? StateName { get; set; }
        public int CountryId { get; set; }
        public string? CountryName { get; set; }
        public int SupplierGroupId { get; set; }
        public string? SupplierGroupName { get; set; }
        public int RegionId { get; set; }
        public string? RegionName { get; set; }
        public string? Address { get; set; }
        public string? PostalCode { get; set; }
        public string Phone { get; set; }
        public string? Website { get; set; }
        public string? Fax { get; set; }
        public string? Email { get; set; }

        public string? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; }
    }
}
