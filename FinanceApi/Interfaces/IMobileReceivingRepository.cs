using System.Data;

public interface IMobileReceivingRepository
{
    Task<(int Id, string PurchaseOrderNo, string? PoLinesJson)?> GetPurchaseOrderAsync(string purchaseOrderNo);
    Task<Dictionary<string, decimal>> GetReceivedQtyMapAsync(int purchaseOrderId);
    Task InsertReceivingAsync(int purchaseOrderId, string purchaseOrderNo, string itemKey, decimal qty, string? createdBy);
}

