using Dapper;
using FinanceApi.Data;
using System.Data;

public class MobileReceivingRepository : IMobileReceivingRepository
{
    private readonly IDbConnectionFactory _dbFactory;
    public MobileReceivingRepository(IDbConnectionFactory dbFactory) => _dbFactory = dbFactory;

    private IDbConnection CreateConn() => _dbFactory.CreateConnection();

    public async Task<(int Id, string PurchaseOrderNo, string? PoLinesJson)?> GetPurchaseOrderAsync(string purchaseOrderNo)
    {
        using var db = CreateConn();
        return await db.QueryFirstOrDefaultAsync<(int, string, string?)>(
            @"SELECT Id, PurchaseOrderNo, PoLines
              FROM PurchaseOrder
              WHERE PurchaseOrderNo=@purchaseOrderNo AND ISNULL(IsActive,1)=1",
            new { purchaseOrderNo });
    }

    public async Task<Dictionary<string, decimal>> GetReceivedQtyMapAsync(int purchaseOrderId)
    {
        using var db = CreateConn();
        var rows = await db.QueryAsync<(string ItemKey, decimal Qty)>(@"
        SELECT ItemKey, SUM(Qty) Qty
        FROM PurchaseOrderReceiving
        WHERE PurchaseOrderId=@purchaseOrderId
        GROUP BY ItemKey", new { purchaseOrderId });

        // ✅ Case-insensitive dictionary + normalize key
        var dict = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
        foreach (var r in rows)
        {
            var key = (r.ItemKey ?? "").Trim().ToUpperInvariant();
            if (!string.IsNullOrWhiteSpace(key))
                dict[key] = r.Qty;
        }
        return dict;
    }


    public async Task InsertReceivingAsync(int purchaseOrderId, string purchaseOrderNo, string itemKey, decimal qty, string? createdBy)
    {
        using var db = CreateConn();
        await db.ExecuteAsync(
            @"INSERT INTO PurchaseOrderReceiving
              (PurchaseOrderId, PurchaseOrderNo, ItemKey, Qty, CreatedBy, CreatedDate)
              VALUES (@purchaseOrderId, @purchaseOrderNo, @itemKey, @qty, @createdBy, GETDATE())",
            new { purchaseOrderId, purchaseOrderNo, itemKey, qty, createdBy });
    }
}
