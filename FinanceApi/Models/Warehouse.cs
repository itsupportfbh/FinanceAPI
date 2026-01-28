using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    public class Warehouse : BaseEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string BinID { get; set; }
        public int CountryId { get; set; }
        public int StateId { get; set; }
        public int CityId { get; set; }
        public string? Address { get; set; }
        public string? Description { get; set; }
        public string Phone { get; set; }


        // Optional navigation properties (not required, but helpful)

        [ForeignKey("CountryId")]
        public Country? Country { get; set; }

        [ForeignKey("StateId")]
        public State? State { get; set; }

        [ForeignKey("CityId")]
        public City? City { get; set; }
    }

    public class WarehouseDto
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string BinID { get; set; }
        public int? CityId { get; set; }
        public string? CityName { get; set; }
        public int? StateId { get; set; }
        public string? StateName { get; set; }
        public int? CountryId { get; set; }
        public string? CountryName { get; set; }
        public string? Address { get; set; }
        public string? Description { get; set; }
        public string Phone { get; set; }


        public string? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; }
        public string BinName { get; set; }
        
    }
}
