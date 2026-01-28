using System.Drawing;
using System.Drawing.Imaging;
using ZXing;
using ZXing.Common;
using ZXing.Windows.Compatibility; // BarcodeWriter (System.Drawing)
using QRCoder;

namespace FinanceApi.Services
{
    public interface ICodeImageService
    {
        byte[] MakeBarcodePng(string text, int width = 520, int height = 140);
        byte[] MakeQrPng(string text, int pixelsPerModule = 8);
    }

    public class CodeImageService : ICodeImageService
    {
        public byte[] MakeBarcodePng(string text, int width = 520, int height = 140)
        {
            if (string.IsNullOrWhiteSpace(text))
                throw new ArgumentException("Barcode text required.", nameof(text));

            var writer = new BarcodeWriter
            {
                Format = BarcodeFormat.CODE_128,
                Options = new EncodingOptions
                {
                    Width = width,
                    Height = height,
                    Margin = 0,
                    PureBarcode = true
                }
            };

            using var bmp = writer.Write(text);       // System.Drawing.Bitmap
            using var ms = new MemoryStream();
            bmp.Save(ms, ImageFormat.Png);
            return ms.ToArray();
        }

        public byte[] MakeQrPng(string text, int pixelsPerModule = 8)
        {
            if (string.IsNullOrWhiteSpace(text))
                throw new ArgumentException("QR text required.", nameof(text));

            var gen = new QRCodeGenerator();
            var data = gen.CreateQrCode(text, QRCodeGenerator.ECCLevel.M);
            using var qr = new QRCode(data);
            using var bmp = qr.GetGraphic(pixelsPerModule, Color.Black, Color.White, true);
            using var ms = new MemoryStream();
            bmp.Save(ms, ImageFormat.Png);
            return ms.ToArray();
        }
    }
}
