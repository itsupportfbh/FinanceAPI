using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IStockTakeService
    {
        Task<IEnumerable<StockTakeDTO>> GetAllAsync();
        Task<IEnumerable<SupplierDto>> GetAllSupplierByWarehouseIdAsync(int id);
        Task<IEnumerable<StockTakeWarehouseItem>> GetWarehouseItemsAsync(long warehouseId, long supplierId,long? strategyId);
        Task<StockTakeDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(StockTake stockTake);
        Task UpdateAsync(StockTake stockTake);
        Task DeleteLicense(int id, int updatedBy);

        Task<int> CreateInventoryAdjustmentsFromStockTakeAsync(
            int stockTakeId,
            string? reason,
            string? remarks,
            string userName,
            bool applyToStock = true,
            bool markPosted = true,
            DateTime? txnDateOverride = null,
            bool onlySelected = true);
    }
}
