namespace FinanceApi.Models
{
    public class SaleLineItems : BaseEntity
    {
        public int Id { get; set; }

        public int SalesId { get; set; }
        public string ProductName { get; set; }

        public string? Description { get; set; }

        public string UnitName { get; set; }
        public int Quantity { get; set; }
        public decimal Rate { get; set; }
        public decimal Total { get; set; }
        public decimal Discount { get; set; }
    }
}
