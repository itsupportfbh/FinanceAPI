using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class StockTakeService : IStockTakeService
    {
        private readonly IStockTakeRepository _repository;

        public StockTakeService(IStockTakeRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<StockTakeDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<IEnumerable<SupplierDto>> GetAllSupplierByWarehouseIdAsync(int id)
        {
            return await _repository.GetAllSupplierByWarehouseIdAsync(id);
        }

        public async Task<IEnumerable<StockTakeWarehouseItem>> GetWarehouseItemsAsync(
       long warehouseId, long supplierId, long? strategyId)
        {
            // normalize ALL
            if (supplierId < 0) supplierId = 0;
            if (strategyId.HasValue && strategyId.Value == 0) strategyId = null;

            var rows = (await _repository.GetWarehouseItemsAsync(warehouseId, supplierId,strategyId))
                       ?.ToList() ?? new List<StockTakeWarehouseItem>();

            // If there are rows and ALL of them are "already checked & posted" → throw coded InvalidOperationException
            if (rows.Count > 0 && rows.All(r =>
                    r.Selected
                 && (r.VarianceQty ?? 0m) == 0m
                 && (r.LineOnHand ?? 0m) == (GetDecimalOrZero(r.OnHand))   // see helper below if OnHand is nullable
            ))
            {
                var ex = new InvalidOperationException("All selected lines for this supplier in this warehouse are already checked & posted.");
                ex.Data["code"] = "AlreadyCheckedPosted";  // <-- REQUIRED for your catch filter
                throw ex;
            }

            // Otherwise, remove the “already posted” rows and return actionable ones
            return rows.Where(r => !(r.Selected && (r.VarianceQty ?? 0m) == 0m &&
                                     (r.LineOnHand ?? 0m) == (GetDecimalOrZero(r.OnHand)))).ToList();

            // local helper (adapt if OnHand is non-nullable)
            static decimal GetDecimalOrZero(object? v)
                => v is decimal d ? d : (v as decimal?) ?? 0m;
        }



        public async Task<StockTakeDTO?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<int> CreateAsync(StockTake stockTake)
        {
            // Example validation logic you might add later
            if (stockTake.LineItems == null || !stockTake.LineItems.Any())
                throw new ArgumentException("At least one line item is required.");

            return await _repository.CreateAsync(stockTake);
        }

        public async Task UpdateAsync(StockTake stockTake)
        {
            await _repository.UpdateAsync(stockTake);
        }

        public async Task DeleteLicense(int id, int updatedBy)
        {
            await _repository.DeactivateAsync(id, updatedBy);
        }

        public Task<int> CreateInventoryAdjustmentsFromStockTakeAsync(
            int stockTakeId, string? reason, string? remarks, string userName,
            bool applyToStock = true, bool markPosted = true, DateTime? txnDateOverride = null, bool onlySelected = true)
        {
            return _repository.CreateFromStockTakeAsync(
                stockTakeId, reason, remarks, userName,
                applyToStock, markPosted, txnDateOverride, onlySelected);
        }
    }
}
