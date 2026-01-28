public class ScanReq
{
    public string PurchaseOrderNo { get; set; } = "";
    public string ItemKey { get; set; } = "";      // scanned barcode or item code
    public decimal Qty { get; set; } = 1;
    public string? CreatedBy { get; set; }
}

public class SyncReq
{
    public string PurchaseOrderNo { get; set; } = "";
    public List<ScanReq> Lines { get; set; } = new();
}

public class PoVm
{
    public int Id { get; set; }
    public string PurchaseOrderNo { get; set; } = "";
    public List<PoLineVm> Lines { get; set; } = new();
}

public class PoLineVm
{
    public string? prNo { get; set; }
    public string? item { get; set; }             // "CS-2681 - White Bread"
    public string? description { get; set; }
    public decimal qty { get; set; }              // ordered qty

    // computed fields
    public decimal receivedQty { get; set; }
    public decimal balanceQty { get; set; }
}
