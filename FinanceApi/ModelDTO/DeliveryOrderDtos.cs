namespace FinanceApi.ModelDTO
{
    public static class DeliveryOrderDtos
    {
        public record DoHeaderDto(
       int Id,
       string DoNumber,
       int Status,
       int? SoId,
       int? PackId,
       string? InvoiceNo,
       int? SiId,
       int? DriverId,
       int? VehicleId,
       string? RouteName,
       DateTime? DeliveryDate,
       string? PodFileUrl,
       bool IsPosted,

       // ✅ NEW
       string? DriverMobileNo,
       string? ReceivedPersonName,
       string? ReceivedPersonMobileNo,

       string? SalesOrderNo,
       int? CustomerId,
       string? CustomerName
   );



        public record DoHeaderEditDto(
            int Id,
            string DoNumber,
            int Status,
            int? SoId,
            int? PackId,
            string? InvoiceNo,
            int? SiId,
            int? DriverId,
            int? VehicleId,
            string? RouteName,
            DateTime? DeliveryDate,
            string? PodFileUrl,
            bool IsPosted,

            // ✅ NEW
            string? DriverMobileNo,
            string? ReceivedPersonName,
            string? ReceivedPersonMobileNo,

            string? SalesOrderNo,
            int? CustomerId,
            string? CustomerName
        );
        public record DoLineDto(
            int Id, int DoId, int? SoLineId, int? PackLineId,
            int? ItemId, string ItemName, string? Uom, decimal Qty, string? Notes,
            string? WarehouseId, string? BinId, string? SupplierId   // ← nullable
        );

        public class DoCreateRequest
        {
            public int? SoId { get; set; }
            public int? PackId { get; set; }
            public int DriverId { get; set; }
            public int? VehicleId { get; set; }
            public string? RouteName { get; set; }
            public DateTime? DeliveryDate { get; set; }
            public string? DriverMobileNo { get; set; }           // snapshot (optional)
            public string? ReceivedPersonName { get; set; }
            public string? ReceivedPersonMobileNo { get; set; }
            public List<DoCreateLine> Lines { get; set; } = new();

            public class DoCreateLine
            {
                public int? SoLineId { get; set; }
                public int? PackLineId { get; set; }
                public int? ItemId { get; set; }
                public string? ItemName { get; set; }
                public string? Uom { get; set; }
                public decimal Qty { get; set; }
                public string? Notes { get; set; }
                public int WarehouseId { get; set; }  // ← nullable
                public int BinId { get; set; }        // ← nullable
                public int SupplierId { get; set; }   // ← nullable
            }
        }

        public class DoAddLineRequest
        {
            public int DoId { get; set; }
            public int? SoLineId { get; set; }
            public int? PackLineId { get; set; }
            public int? ItemId { get; set; }
            public string? ItemName { get; set; }
            public string? Uom { get; set; }
            public decimal Qty { get; set; }
            public string? Notes { get; set; }
            public string? WarehouseId { get; set; }  // ← nullable
            public string? BinId { get; set; }        // ← nullable
            public string? SupplierId { get; set; }   // ← nullable
        }

       public class DoUpdateHeaderRequest
{
  public int? DriverId { get; set; }
  public int? VehicleId { get; set; }
  public string? RouteName { get; set; }
  public DateTime? DeliveryDate { get; set; }

  // ✅ NEW
  public string? DriverMobileNo { get; set; }
  public string? ReceivedPersonName { get; set; }
  public string? ReceivedPersonMobileNo { get; set; }
}

    }
}
