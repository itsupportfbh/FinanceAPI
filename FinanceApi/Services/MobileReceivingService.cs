using System.Text.Json;

public class MobileReceivingService : IMobileReceivingService
{
    private readonly IMobileReceivingRepository _repo;
    public MobileReceivingService(IMobileReceivingRepository repo) => _repo = repo;

    // normalize any scanned key or po line text to ItemCode
    private static string ItemCodeFrom(string? value)
    {
        var s = (value ?? "").Trim();
        if (string.IsNullOrWhiteSpace(s)) return "";

        // "CS-2681 - White Bread" -> "CS-2681"
        var code = s.Split(" - ")[0].Trim();
        return code.ToUpperInvariant();
    }

    private static List<PoLineVm> ParseLines(string? poLinesJson)
    {
        if (string.IsNullOrWhiteSpace(poLinesJson)) return new List<PoLineVm>();

        return JsonSerializer.Deserialize<List<PoLineVm>>(
                   poLinesJson,
                   new JsonSerializerOptions { PropertyNameCaseInsensitive = true }
               ) ?? new List<PoLineVm>();
    }

    public async Task<PoVm> GetPurchaseOrderAsync(string purchaseOrderNo)
    {
        var po = await _repo.GetPurchaseOrderAsync(purchaseOrderNo)
                 ?? throw new Exception("PO not found");

        var lines = ParseLines(po.PoLinesJson);

        var receivedMap = await _repo.GetReceivedQtyMapAsync(po.Id);

        foreach (var l in lines)
        {
            var code = ItemCodeFrom(l.item);
            l.receivedQty = receivedMap.TryGetValue(code, out var r) ? r : 0;
            l.balanceQty = l.qty - l.receivedQty;

        }

        return new PoVm
        {
            Id = po.Id,
            PurchaseOrderNo = po.PurchaseOrderNo,
            Lines = lines
        };
    }

    public async Task ValidateScanAsync(ScanReq request)
    {
        if (string.IsNullOrWhiteSpace(request.PurchaseOrderNo))
            throw new Exception("PO required");

        if (string.IsNullOrWhiteSpace(request.ItemKey))
            throw new Exception("Barcode required");

        if (request.Qty <= 0)
            throw new Exception("Qty must be > 0");

        var po = await _repo.GetPurchaseOrderAsync(request.PurchaseOrderNo.Trim())
                 ?? throw new Exception("PO not found");

        var lines = ParseLines(po.PoLinesJson);

        var scanCode = ItemCodeFrom(request.ItemKey); // "CS-2681"
        if (string.IsNullOrWhiteSpace(scanCode))
            throw new Exception("Invalid item code");

        // find matching PO line by item code
        var line = lines.FirstOrDefault(x => ItemCodeFrom(x.item).Equals(scanCode, StringComparison.OrdinalIgnoreCase));
        if (line == null)
            throw new Exception("Item not in PO");

        // check over receiving
        var receivedMap = await _repo.GetReceivedQtyMapAsync(po.Id);
        var receivedQty = receivedMap.TryGetValue(scanCode, out var r) ? r : 0;

        if (receivedQty + request.Qty > line.qty)
            throw new Exception("Over receiving not allowed");
    }

    public async Task SyncAsync(SyncReq request)
    {
        if (string.IsNullOrWhiteSpace(request.PurchaseOrderNo))
            throw new Exception("PO required");

        if (request.Lines == null || request.Lines.Count == 0)
            throw new Exception("No lines to sync");

        var po = await _repo.GetPurchaseOrderAsync(request.PurchaseOrderNo.Trim())
                 ?? throw new Exception("PO not found");

        foreach (var l in request.Lines)
        {
            // force PO number from header
            l.PurchaseOrderNo = request.PurchaseOrderNo;

            // validate first
            await ValidateScanAsync(l);

            // store ItemKey as ItemCode only
            var itemCode = ItemCodeFrom(l.ItemKey);

            await _repo.InsertReceivingAsync(
                po.Id,
                request.PurchaseOrderNo,
                itemCode,   // ✅ store CS-2681 only
                l.Qty,
                l.CreatedBy
            );

        }
    }
}
