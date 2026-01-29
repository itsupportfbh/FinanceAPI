using System;
using System.Collections.Generic;

namespace FinanceApi.ModelDTO
{
    public class QutationDetailsViewInfo
    {
        // =========================
        // HEADER (1st SELECT)
        // =========================
        public int Id { get; set; }
        public string Number { get; set; } = "";
        public QuotationStatus Status { get; set; } = QuotationStatus.Draft;

        public int CustomerId { get; set; }
        public string? CustomerName { get; set; }

        public int CurrencyId { get; set; }
        public decimal FxRate { get; set; } = 1m;

        public int PaymentTermsId { get; set; }
        public string PaymentTermsName { get; set; } = "";

        public DateTime? DeliveryDate { get; set; }

        public decimal Subtotal { get; set; }
        public decimal TaxAmount { get; set; }
        public decimal Rounding { get; set; }
        public decimal GrandTotal { get; set; }

        public bool NeedsHodApproval { get; set; }

        public string CurrencyName { get; set; } = "";
        public string DeliveryTo { get; set; } = "";
        public string Remarks { get; set; } = "";

        public decimal GstPct { get; set; }

        // ✅ ItemSet summary (comes from OUTER APPLY in header)
        public int ItemSetCount { get; set; }
        public string? ItemSetIds { get; set; }        // CSV
        public string? ItemSetsJson { get; set; } 
        
        public int LineSourceId { get; set; }

        // =========================
        // CHILD COLLECTIONS
        // =========================
        public List<QuotationLineDetailsViewInfo> Lines { get; set; } = new();

        // ✅ 3rd SELECT result set (QuotationItemSet list)
        public List<QuotationItemSetDetailsViewInfo> ItemSets { get; set; } = new();

        // =====================================================
        // LINES (2nd SELECT)
        // =====================================================
        public class QuotationLineDetailsViewInfo
        {
            public int Id { get; set; }
            public int QuotationId { get; set; }

            public int ItemId { get; set; }
            public string? ItemName { get; set; }

            public int UomId { get; set; }
            public string? UomName { get; set; }

            public decimal Qty { get; set; }
            public decimal UnitPrice { get; set; }
            public decimal DiscountPct { get; set; }

            public int? TaxCodeId { get; set; }

            // Your query returns l.TaxMode (string) -> ex: "EXCLUSIVE" / "INCLUSIVE" / "Standard-Rated"
            public string? TaxMode { get; set; }

            public decimal? LineNet { get; set; }
            public decimal? LineTax { get; set; }
            public decimal? LineTotal { get; set; }

            public string? Description { get; set; }

            // Warehouse aggregation (from OUTER APPLY whAgg)
            public int WarehouseCount { get; set; }
            public string? WarehouseIds { get; set; }     // CSV
            public string? WarehousesJson { get; set; }   // JSON string

            // ✅ Deserialized from WarehousesJson
            public List<WarehouseInfoDTO> Warehouses { get; set; } = new();
        }

        // =====================================================
        // ITEM SET DETAILS (3rd SELECT)
        // =====================================================
        public class QuotationItemSetDetailsViewInfo
        {
            public int Id { get; set; }
            public int QuotationId { get; set; }

            public int ItemSetId { get; set; }
            public string? ItemSetName { get; set; }

            public int? CreatedBy { get; set; }
            public DateTime? CreatedDate { get; set; }

            public bool? IsActive { get; set; }
        }

        // =====================================================
        // WAREHOUSE JSON OBJECT (inside WarehousesJson)
        // =====================================================
        public class WarehouseInfoDTO
        {
            public int WarehouseId { get; set; }
            public string WarehouseName { get; set; } = "";

            public decimal OnHand { get; set; }
            public decimal Reserved { get; set; }
            public decimal Available { get; set; }
        }
    }

    // ✅ Example enum (if already exists, remove this duplicate)

}
