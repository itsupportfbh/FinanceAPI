using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class StockAdjustmentServices : IStockAdjustmentServices
    {
        private readonly IStockAdjustmentRepository _repository;

        public StockAdjustmentServices(IStockAdjustmentRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<BinDTO>> GetBinDetailsbywarehouseID(int id)
        {
            return await _repository.GetBinDetailsbywarehouseID(id);
        }

        public async Task<IEnumerable<StockAdjustmentDTO>> GetItemDetailswithwarehouseandBinID(int warehouseId, int binId)
        {
            return await _repository.GetItemDetailswithwarehouseandBinID(warehouseId,binId);
        }
    }
}
