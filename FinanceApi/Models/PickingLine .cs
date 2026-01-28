namespace FinanceApi.Models
{
    public class PickingLine : BaseEntity
    {
        public int Id { get; set; }
        public int PickId { get; set; }
        public int SoLineId { get; set; }
        public long ItemId { get; set; }

        public int WarehouseId { get; set; }
        public int? SupplierId { get; set; }
        public int? BinId { get; set; }

        public decimal Quantity { get; set; }
        public int? CartonId { get; set; }
    }
}
