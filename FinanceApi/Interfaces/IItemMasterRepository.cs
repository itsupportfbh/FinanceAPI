using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace Interfaces
{
    public interface IItemMasterRepository
    {
        Task<IEnumerable<ItemDto>> GetAllAsync();
        Task<getItemMasterDTO?> GetByIdAsync(int id);
        Task<long> CreateAsync(ItemMasterUpsertDto dto);
        Task UpdateAsync(ItemMasterUpsertDto dto);
        Task DeactivateAsync(int id);

        Task<IEnumerable<ItemWarehouseStockDTO>> GetAuditsByItemAsync(int itemId);
        Task<IEnumerable<ItemWarehouseStockDTO>> GetWarehouseStockByItemAsync(int itemId);
        Task<IEnumerable<ItemWarehouseStockDTO>> GetSupplierPricesByItemAsync(int itemId);
        Task<BomSnapshot> GetBomSnapshotAsync(long itemId);

        Task ApplyGrnToInventoryAsync(ApplyGrnRequest req);

        // ✅ add this missing repo contract
        Task UpdateWarehouseAndSupplierPriceAsync(UpdateWarehouseSupplierPriceDto dto);

        Task<IEnumerable<StockAdjustmentItemsDTO?>> GetItemDetailsByItemId(int id);

    }
}
