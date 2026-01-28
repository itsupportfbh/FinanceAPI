using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IItemMasterService
    {
        Task<IEnumerable<ItemDto>> GetAllAsync();
        Task<getItemMasterDTO?> GetByIdAsync(int id);
        Task<long> CreateAsync(ItemMasterUpsertDto dto);
        Task UpdateAsync(ItemMasterUpsertDto dto);
        Task DeleteAsync(int id);

        Task<IEnumerable<ItemWarehouseStockDTO>> getStockByItemId(int itemId);
        Task<IEnumerable<ItemWarehouseStockDTO>> getPriceByItemId(int itemId);
        Task<IEnumerable<ItemWarehouseStockDTO>> getAuditByItemId(int itemId);
        Task<BomSnapshot> GetBomSnapshot(int itemId);

        Task ApplyGrnToInventoryAsync(ApplyGrnRequest req);

        // ✅ add this missing service contract
        Task UpdateWarehouseAndSupplierPriceAsync(UpdateWarehouseSupplierPriceDto dto);

        Task<IEnumerable<StockAdjustmentItemsDTO>> GetItemDetailsByItemId(int id);
    }
}
