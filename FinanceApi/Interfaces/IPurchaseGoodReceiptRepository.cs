using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IPurchaseGoodReceiptRepository
    {
        Task<IEnumerable<PurchaseGoodReceiptItemsDTO>> GetAllAsync();

        Task<PurchaseGoodReceiptItemsDTO> GetByIdAsync(long id);
        Task<int> CreateAsync(PurchaseGoodReceiptItems goodReceiptItemsDTO);

        Task<IEnumerable<PurchaseGoodReceiptItemsViewInfo>> GetAllDetailsAsync();

        Task UpdateAsync(PurchaseGoodReceiptItems purchaseGoodReceipt);

        Task DeactivateAsync(int id);
        Task<IEnumerable<PurchaseGoodReceiptItemsDTO>> GetAllGRNByPoId();

        Task<ApplyGrnResult> ApplyGrnAndUpdateSalesOrderAsync(
       string itemCode, int? warehouseId, int? supplierId, int? binId, decimal receivedQty);
    }
}
