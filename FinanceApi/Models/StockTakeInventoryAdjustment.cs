namespace FinanceApi.Models
{
    public class StockTakeInventoryAdjustment : BaseEntity
    {
        public long Id { get; set; }

        // What & Where
        public int ItemId { get; set; }
        public int WarehouseTypeId { get; set; }
        public int BinId { get; set; }
        // public int? BinId { get; set; } // Optional if you later support bins

        // When
        public DateTime TxnDate { get; set; } = DateTime.UtcNow;

        // Why
        public int ReasonId { get; set; }
        public string? Remarks { get; set; }

        // Source
        public string SourceType { get; set; } = "StockTake";  // e.g., "StockTake", "Manual"
        public int SourceId { get; set; }                      // StockTake.Id
        public int? SourceLineId { get; set; }                // StockTakeLines.Id

        // Quantities
        public decimal QtyIn { get; set; }
        public decimal CountedQty { get; set; }
        public decimal BadCountedQty { get; set; }
        public decimal QtyOut { get; set; }

        // Audit snapshot
        public decimal? QtyBefore { get; set; }
        public decimal? QtyAfter { get; set; }

        // Cost hooks (optional)
        public decimal? UnitCost { get; set; }
        public decimal? ExtCost { get; set; }
    }
}
