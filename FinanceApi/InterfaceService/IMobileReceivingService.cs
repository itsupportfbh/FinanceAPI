public interface IMobileReceivingService
{
    Task<PoVm> GetPurchaseOrderAsync(string purchaseOrderNo);
    Task ValidateScanAsync(ScanReq request);
    Task SyncAsync(SyncReq request);
}
