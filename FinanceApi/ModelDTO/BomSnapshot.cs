public sealed class BomLatestRow
{
    public long SupplierId { get; set; }
    public string SupplierName { get; set; } = "";
    public decimal ExistingCost { get; set; }
    public decimal UnitCost { get; set; }
    public DateTime CreatedDate { get; set; }
}

public sealed class BomHistoryPoint
{
    public long SupplierId { get; set; }
    public string SupplierName { get; set; } = "";
    public decimal ExistingCost { get; set; }
    public decimal UnitCost { get; set; }
    public DateTime CreatedDate { get; set; }
    public int Rn { get; set; }
}

public sealed class BomSnapshot
{
    public List<BomLatestRow> Latest { get; set; } = new();
    public List<BomHistoryPoint> History { get; set; } = new();
}
