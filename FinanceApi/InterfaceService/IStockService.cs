using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;

namespace FinanceApi.InterfaceService
{
    public interface IStockService
    {
        Task<IEnumerable<StockDTO>> GetAllAsync();
        Task<int> InsertBulkAsync(IEnumerable<Stock> stocks);
        Task<StockDTO> GetById(long id);
        Task Update(Stock stock);
        Task Delete(int id);

        Task<IEnumerable<StockListViewInfo>> GetAllStockList();
        Task<int> MarkAsTransferredBulkAsync(IEnumerable<MarkAsTransferredRequest> requests);
        Task<IEnumerable<MaterialTransferListViewInfo>> GetAllStockTransferedList();

        Task<int> AdjustOnHandAsync(AdjustOnHandRequest request);

        Task<IEnumerable<StockListViewInfo>> GetAllItemStockList();

        Task ApproveTransfersBulkAsync(IEnumerable<TransferApproveRequest> transfers);

        Task<StockHistoryViewInfo> GetByIdStockHistory(long id);

        Task<IEnumerable<StockTransferListViewInfo>> GetStockTransferedList();



        Task<IEnumerable<int>> GetTransferredMrIdsAsync();



        Task<IEnumerable<MaterialTransferListViewInfo>> GetMaterialTransferList();
    }
}
