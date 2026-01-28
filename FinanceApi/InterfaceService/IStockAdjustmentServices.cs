using FinanceApi.ModelDTO;

namespace FinanceApi.InterfaceService
{
    public interface IStockAdjustmentServices
    {
        Task<IEnumerable<BinDTO>> GetBinDetailsbywarehouseID(int id);
        Task<IEnumerable<StockAdjustmentDTO>> GetItemDetailswithwarehouseandBinID(int warehouseId, int binId);
    }
}
