using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IPurchaseGoodReceiptService
    {
        Task<IEnumerable<PurchaseGoodReceiptItemsDTO>> GetAllAsync();

        Task<int> CreateAsync(PurchaseGoodReceiptItems goodReceiptItemsDTO);

        Task<PurchaseGoodReceiptItemsDTO> GetById(long id);

        Task<IEnumerable<PurchaseGoodReceiptItemsViewInfo>> GetAllGRNDetailsAsync();

        Task UpdateAsync(PurchaseGoodReceiptItems purchaseGoodReceipt);

        Task DeleteAsync(int id);
        Task<IEnumerable<PurchaseGoodReceiptItemsDTO>> GetAllGRNByPoId();

        Task<ApplyGrnResult> ApplyGrnAndUpdateSalesOrderAsync(ApplyGrnAndSalesOrderRequest request);
    }
}
