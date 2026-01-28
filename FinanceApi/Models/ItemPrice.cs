using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    [Table("ItemPrice")]
    public class ItemPrice
    {
        public long? Id { get; set; }
        public long SupplierId { get; set; }
        public decimal Price { get; set; }
        public int WarehouseId { get; set; }
    }
}
