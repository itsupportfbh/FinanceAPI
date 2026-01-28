namespace FinanceApi.ModelDTO
{
    public class AllocationPreviewRequest
    {
       
        
            public List<AllocPreviewLine> Lines { get; set; } = new();
        
        public class AllocPreviewLine
        {
            public int ItemId { get; set; }
            public decimal Quantity { get; set; }
        }

        public class AllocationPreviewResponse
        {
            public List<AllocationPreviewLineResult> Lines { get; set; } = new();
        }
        public class AllocationPreviewLineResult
        {
            public int ItemId { get; set; }
            public decimal RequestedQty { get; set; }
            public decimal AllocatedQty { get; set; }
            public bool FullyAllocated { get; set; }
            public List<AllocPiece> Allocations { get; set; } = new();
        }
        public class AllocPiece
        {
            public int WarehouseId { get; set; }
            public int SupplierId { get; set; }
            public int? BinId { get; set; }   // <-- add this
            public decimal Qty { get; set; }
            public string WarehouseName { get; set; }
            public string SupplierName { get; set; }
            public string BinName { get; set; }
        }

    }
}
