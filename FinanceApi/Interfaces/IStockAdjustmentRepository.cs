using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface IStockAdjustmentRepository
    {
        Task<IEnumerable<BinDTO>> GetBinDetailsbywarehouseID(int id);

        Task<IEnumerable<StockAdjustmentDTO>> GetItemDetailswithwarehouseandBinID(int warehouseId, int binId);
    }
}
