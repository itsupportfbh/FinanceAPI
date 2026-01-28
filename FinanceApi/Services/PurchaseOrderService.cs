using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using FinanceApi.Repositories;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using static QRCoder.PayloadGenerator;

namespace FinanceApi.Services
{
    public class PurchaseOrderService : IPurchaseOrderService
    {
        private readonly IPurchaseOrderRepository _repository;
        private readonly IConfiguration _config;
        private readonly ICodeImageService _img;
        private readonly IMobileLinkTokenService _tokenSvc;
        private readonly Interfaces.IEmailService _emailService;


        public PurchaseOrderService(IPurchaseOrderRepository repository, IConfiguration config, ICodeImageService img,
            IMobileLinkTokenService tokenSvc, Interfaces.IEmailService emailService)
        {
            _repository = repository;
            _config = config;
            _img = img;
            _tokenSvc = tokenSvc;
            _emailService = emailService;
        }

        public async Task<IEnumerable<PurchaseOrderDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<IEnumerable<PurchaseOrderDto>> GetAllDetailswithGRN()
        {
            return await _repository.GetAllDetailswithGRN();
        }

        public async Task<PurchaseOrderDto?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<int> CreateAsync(PurchaseOrder purchaseOrder)
        {
            // Example validation logic you might add later
            if (purchaseOrder.PoLines == null || !purchaseOrder.PoLines.Any())
                throw new ArgumentException("At least one line item is required.");

            return await _repository.CreateAsync(purchaseOrder);
        }

        public async Task UpdateAsync(PurchaseOrder purchaseOrder)
        {
            await _repository.UpdateAsync(purchaseOrder);
        }

        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }
        public PoQrResponse BuildPoQr(string poNo)
        {
            // Put your real UI base URL here (NOT Angular :4200 in production)
            var baseUrl = _config["PublicUiBaseUrl"] ?? "http://192.168.6.192:4200";
            var token = _tokenSvc.Generate(poNo, minutes: 15);

            var payloadUrl = $"{baseUrl}/purchase/mobilereceiving?poNo={Uri.EscapeDataString(poNo)}&t={token}";

            var qrPng = _img.MakeQrPng(payloadUrl, pixelsPerModule: 8);

            string ToDataUrl(byte[] bytes) => $"data:image/png;base64,{Convert.ToBase64String(bytes)}";

            return new PoQrResponse(
                PurchaseOrderNo: poNo,
                QrPayloadUrl: payloadUrl,
                QrCodeSrcBase64: ToDataUrl(qrPng)
            );
        }


        public async Task<ResponseResult> EmailSupplierPoAsync(int poId, IFormFile pdf)
        {
            if (pdf == null || pdf.Length == 0)
                return new ResponseResult(false, "PDF is required", null);

            var meta = await _repository.GetSupplierEmailMetaAsync(poId);

            if (string.IsNullOrWhiteSpace(meta.Email))
                return new ResponseResult(false, "Supplier email not found", null);

            // ✅ Approved only (2 = Approved)
            if (meta.ApprovalStatus != 2)
                return new ResponseResult(false, "PO not approved", null);

            byte[] pdfBytes;
            using (var ms = new MemoryStream())
            {
                await pdf.CopyToAsync(ms);
                pdfBytes = ms.ToArray();
            }

            await _emailService.SendSupplierPoEmailAsync(
                meta.Email,
                meta.SupplierName,
                meta.PoNo,
                pdfBytes
            );

            return new ResponseResult(true, "PO emailed to supplier", null);
        }

    }
}
