namespace FinanceApi.Models
{
    public record CodesRequest(int SoId, DateTime? SoDateUtc = null, string Country = "SG", string Prefix = "PKL");

    public record CodesResponseEx(
        string BarCode,
        string QrText,
        string BarCodeSrcBase64,
        string QrCodeSrcBase64
    );
}