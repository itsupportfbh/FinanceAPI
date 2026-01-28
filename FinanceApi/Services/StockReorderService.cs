using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class StockReorderService : IStockReorderService
    {
        private readonly IStockReorderRepository _repository;

        public StockReorderService(IStockReorderRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<StockReorderDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<IEnumerable<StockReorderWarehouseItems>> GetWarehouseItemsAsync(
                        long warehouseId)
        {

            return await _repository.GetWarehouseItemsAsync(warehouseId);
        }

        public async Task<StockReorderDTO?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<int> CreateAsync(StockReorder stockReorder)
        {
            // Example validation logic you might add later
            if (stockReorder.LineItems == null || !stockReorder.LineItems.Any())
                throw new ArgumentException("At least one line item is required.");

            return await _repository.CreateAsync(stockReorder);
        }

        public async Task UpdateAsync(StockReorder stockReorder)
        {
            await _repository.UpdateAsync(stockReorder);
        }

        public async Task DeleteLicense(int id, int updatedBy)
        {
            await _repository.DeactivateAsync(id, updatedBy);
        }
        public async Task<IEnumerable<ReorderPreviewLine>> GetReorderPreviewAsync(int stockReorderId)
        {
           return await _repository.GetReorderPreviewAsync(stockReorderId);
        }
    }
}
