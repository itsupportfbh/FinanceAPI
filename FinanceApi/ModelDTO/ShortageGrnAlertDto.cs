public class ShortageGrnAlertDto
{
    public int ProductionPlanId { get; set; }
    public int? SalesOrderId { get; set; }

    public int GrnId { get; set; }
    public string? GrnNo { get; set; }
    public DateTime? ReceptionDate { get; set; }

    public string? ItemCode { get; set; }
    public string? ItemName { get; set; }
    public decimal QtyReceived { get; set; }
}
