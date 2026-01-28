using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IStockReorderService
    {
        Task<IEnumerable<StockReorderDTO>> GetAllAsync();
        Task<IEnumerable<StockReorderWarehouseItems>> GetWarehouseItemsAsync(long warehouseId);
        Task<StockReorderDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(StockReorder stockReorde);
        Task UpdateAsync(StockReorder stockReorde);
        Task DeleteLicense(int id, int updatedBy);
        Task<IEnumerable<ReorderPreviewLine>> GetReorderPreviewAsync(int stockReorderId);
    }
}
