namespace FinanceApi.ModelDTO
{
    public record PoQrResponse(
      string PurchaseOrderNo,
      string QrPayloadUrl,
      string QrCodeSrcBase64
  );

}
