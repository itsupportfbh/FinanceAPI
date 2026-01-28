// FinanceApi/ModelDTO/StockReorderDTO.cs
using FinanceApi.Models;
using System;
using System.Collections.Generic;

namespace FinanceApi.ModelDTO
{
    public class StockReorderDTO : BaseEntity
    {
        public int Id { get; set; }
        public int WarehouseTypeId { get; set; }
        public string? WarehouseName { get; set; }

        public int MethodId { get; set; }

        // Make sure repo aliases HorizonDays -> Horizon, IncludeLeadTime -> LeadTime
        public int Horizon { get; set; }
        public bool LeadTime { get; set; }

        public StockReorderStatus Status { get; set; } = StockReorderStatus.Draft;
        public List<StockReorderLines> LineItems { get; set; } = new();
    }

    public enum StockReorderStatus
    {
        Draft = 1,
        Approved = 2,
        Posted = 3
    }

    public enum StockReorderLineStatus { Draft = 1, Approved = 2, Posted = 3 }

    // Modal preview line DTO (from PRLines + StockReorderLines)
    public sealed class ReorderPreviewLine
    {
        public string PrNo { get; set; } = "";
        public int ItemId { get; set; }
        public string ItemCode { get; set; } = "";
        public string ItemName { get; set; } = "";
        public decimal RequestedQty { get; set; }

        public int? SupplierId { get; set; }
        public int? WarehouseId { get; set; }
        public string? Location { get; set; }
        public DateTime? DeliveryDate { get; set; }

        public decimal OnHand { get; set; }
        public decimal MinQty { get; set; }
        public decimal MaxQty { get; set; }
        public decimal ReorderQty { get; set; }
        public int Status {  get; set; }
    }
}
