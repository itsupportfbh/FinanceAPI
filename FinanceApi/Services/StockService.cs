using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;

namespace FinanceApi.Services
{
    public class StockService : IStockService
    {
        private readonly IStockRepository _repository;

        public StockService(IStockRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<StockDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> InsertBulkAsync(IEnumerable<Stock> stocks)
        {
            return await _repository.InsertBulkAsync(stocks);
        }


        public async Task<StockDTO> GetById(long id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task Update(Stock stock)
        {
            return _repository.UpdateAsync(stock);
        }


        public async Task Delete(int id)
        {
            await _repository.DeactivateAsync(id);
        }

        public async Task<IEnumerable<StockListViewInfo>> GetAllStockList()
        {
            return await _repository.GetAllStockList();
        }

        public async Task<IEnumerable<StockListViewInfo>> GetAllItemStockList()
        {
            return await _repository.GetAllItemStockList();
        }

        public async Task<int> MarkAsTransferredBulkAsync(IEnumerable<MarkAsTransferredRequest> requests)
        {
            return await _repository.MarkAsTransferredBulkAsync(requests);
        }

        public async Task<IEnumerable<MaterialTransferListViewInfo>> GetAllStockTransferedList()
        {
            return await _repository.GetAllStockTransferedList();
        }
        public async Task<IEnumerable<StockTransferListViewInfo>> GetStockTransferedList()
        {
            return await _repository.GetStockTransferedList();
        }

        public async Task<int> AdjustOnHandAsync(AdjustOnHandRequest request)
        {
            return await _repository.AdjustOnHandAsync(request);
        }

        public async Task ApproveTransfersBulkAsync(IEnumerable<TransferApproveRequest> transfers)
        {
            if (transfers == null) throw new ArgumentNullException(nameof(transfers));

            var list = transfers.ToList();
            if (!list.Any()) throw new ArgumentException("Transfer list is empty.");

            foreach (var t in list)
            {
                if (t.StockId <= 0) throw new ArgumentException("StockId is required.");
                if (t.ItemId <= 0) throw new ArgumentException("ItemId is required.");
                if (t.WarehouseId <= 0) throw new ArgumentException("From WarehouseId is required.");
                if (t.ToWarehouseId <= 0) throw new ArgumentException("To WarehouseId is required.");
                if (t.ToBinId <= 0) throw new ArgumentException("ToBinId is required.");
                if (t.TransferQty <= 0) throw new ArgumentException("TransferQty must be > 0.");
                if (t.RequestedQty < 0) t.RequestedQty = 0;
            }

            await _repository.ApproveTransfersBulkAsync(list);
        }

        public async Task<StockHistoryViewInfo> GetByIdStockHistory(long id)
        {
            return await _repository.GetByIdStockHistory(id);
        }

        public async Task<IEnumerable<int>> GetTransferredMrIdsAsync()
        {
            return await _repository.GetTransferredMrIdsAsync();
        }

        public async Task<IEnumerable<MaterialTransferListViewInfo>> GetMaterialTransferList()
        {
            return await _repository.GetMaterialTransferList();
        }


    }
}
