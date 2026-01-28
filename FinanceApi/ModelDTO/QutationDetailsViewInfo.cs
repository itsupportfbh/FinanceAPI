namespace FinanceApi.ModelDTO
{
    public class QutationDetailsViewInfo
    {
        public int? Id { get; set; }
        public string Number { get; set; } = "";
        public QuotationStatus Status { get; set; } = QuotationStatus.Draft;

        public int CustomerId { get; set; }
        public string? CustomerName { get; set; }

        public int CurrencyId { get; set; }
        public decimal FxRate { get; set; } = 1m;
        public int PaymentTermsId { get; set; }
        public DateTime? DeliveryDate { get; set; }

        public decimal Subtotal { get; set; }
        public decimal TaxAmount { get; set; }
        public decimal Rounding { get; set; }
        public decimal GrandTotal { get; set; }
        public bool NeedsHodApproval { get; set; }

        public string CurrencyName { get; set; } = "";
        public string PaymentTermsName { get; set; } = "";
        public decimal GstPct { get; set; }

        public string DeliveryTo { get; set; }
        public string Remarks { get; set; }
        public List<QuotationLineDetailsViewInfo> Lines { get; set; } = new();

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
            public string? TaxMode { get; set; } = "EXCLUSIVE";

            public string Description { get; set; }

            public decimal? LineNet { get; set; }
            public decimal? LineTax { get; set; }
            public decimal? LineTotal { get; set; }

            public int WarehouseCount { get; set; }
            public string? WarehouseIds { get; set; } // csv
            public string? WarehousesJson { get; set; }
            public List<WarehouseInfoDTO>? Warehouses { get; set; } = new();
        }

        public class WarehouseInfoDTO
        {
            public int WarehouseId { get; set; }
            public string WarehouseName { get; set; } = "";
            public decimal OnHand { get; set; }
            public decimal Reserved { get; set; }
            public decimal Available { get; set; }
        }
    }
}
