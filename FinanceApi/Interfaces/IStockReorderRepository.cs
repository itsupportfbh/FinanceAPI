using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IStockReorderRepository
    {
        Task<IEnumerable<StockReorderDTO>> GetAllAsync();
        Task<IEnumerable<StockReorderWarehouseItems>> GetWarehouseItemsAsync(long warehouseId);
        Task<StockReorderDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(StockReorder stockReorder);
        Task UpdateAsync(StockReorder stockReorder);
        Task DeactivateAsync(int id, int updatedBy);
        Task<IEnumerable<ReorderPreviewLine>> GetReorderPreviewAsync(int stockReorderId);
}
}
