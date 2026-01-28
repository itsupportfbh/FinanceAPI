using FinanceApi.Data;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IPurchaseOrderService
    {    
        Task<IEnumerable<PurchaseOrderDto>> GetAllAsync();
        Task<PurchaseOrderDto> GetByIdAsync(int id);
        Task<int> CreateAsync(PurchaseOrder purchaseOrder);
        Task UpdateAsync(PurchaseOrder purchaseOrder);
        Task DeleteLicense(int id);

        Task<IEnumerable<PurchaseOrderDto>> GetAllDetailswithGRN();
        PoQrResponse BuildPoQr(string poNo);

        Task<ResponseResult> EmailSupplierPoAsync(int poId, IFormFile pdf);
    }
}
