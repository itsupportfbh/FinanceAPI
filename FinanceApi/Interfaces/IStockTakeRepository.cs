using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IStockTakeRepository
    {
        Task<IEnumerable<StockTakeDTO>> GetAllAsync();
        Task<IEnumerable<SupplierDto>> GetAllSupplierByWarehouseIdAsync(int id);
        Task<IEnumerable<StockTakeWarehouseItem>> GetWarehouseItemsAsync(long warehouseId,long supplierId,long? strategyId);
        Task<StockTakeDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(StockTake stockTake);
        Task UpdateAsync(StockTake stockTake);
        Task DeactivateAsync(int id, int updatedBy);
        // In your repository interface:
        Task<int> CreateFromStockTakeAsync(
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
