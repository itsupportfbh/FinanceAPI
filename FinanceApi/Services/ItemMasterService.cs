using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Interfaces;


namespace FinanceApi.Services
{
    public class ItemMasterService : IItemMasterService
    {
        private readonly IItemMasterRepository _repo;
        public ItemMasterService(IItemMasterRepository repo) => _repo = repo;

        public Task<IEnumerable<ItemDto>> GetAllAsync() => _repo.GetAllAsync();
        public Task<getItemMasterDTO?> GetByIdAsync(int id) => _repo.GetByIdAsync(id);
        public Task<long> CreateAsync(ItemMasterUpsertDto dto) => _repo.CreateAsync(dto);
        public Task UpdateAsync(ItemMasterUpsertDto dto) => _repo.UpdateAsync(dto);
        public Task DeleteAsync(int id) => _repo.DeactivateAsync(id);

        public Task<IEnumerable<ItemWarehouseStockDTO>> getStockByItemId(int itemId)
            => _repo.GetWarehouseStockByItemAsync(itemId);

        // ✅ make the return types consistent (non-nullable element type)
        public Task<IEnumerable<ItemWarehouseStockDTO>> getPriceByItemId(int itemId)
            => _repo.GetSupplierPricesByItemAsync(itemId);

        public Task<IEnumerable<ItemWarehouseStockDTO>> getAuditByItemId(int itemId)
            => _repo.GetAuditsByItemAsync(itemId);

        public Task<BomSnapshot> GetBomSnapshot(int itemId)
            => _repo.GetBomSnapshotAsync(itemId);

        public Task ApplyGrnToInventoryAsync(ApplyGrnRequest req)
            => _repo.ApplyGrnToInventoryAsync(req);

        // ✅ pass-through to repo
        public Task UpdateWarehouseAndSupplierPriceAsync(UpdateWarehouseSupplierPriceDto dto)
            => _repo.UpdateWarehouseAndSupplierPriceAsync(dto);


        public Task<IEnumerable<StockAdjustmentItemsDTO>> GetItemDetailsByItemId(int id)
           => _repo.GetItemDetailsByItemId(id);
    }
}
