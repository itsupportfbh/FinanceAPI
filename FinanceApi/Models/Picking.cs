namespace FinanceApi.Models
{
    public class Picking : BaseEntity
    {
        public int Id { get; set; }
        public int SoId { get; set; }
        public DateTime? SoDate { get; set; }
        public DateTime? DeliveryDate { get; set; }       
        public string? BarCode { get; set; }
        public string? QrCode { get; set; }
        public byte[]? BarCodeSrc { get; set; }  // PNG bytes
        public byte[]? QrCodeSrc { get; set; }   // PNG bytes

        public byte Status { get; set; } = 0;

        public List<PickingLine> LineItems { get; set; } = new();
    }
}
