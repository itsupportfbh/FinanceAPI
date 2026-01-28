using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class PurchaseGoodReceiptService : IPurchaseGoodReceiptService
    {
        private readonly IPurchaseGoodReceiptRepository _repository;
        private readonly IPeriodCloseService _periodClose;   // 🔸 NEW

        public PurchaseGoodReceiptService(
            IPurchaseGoodReceiptRepository repository,
            IPeriodCloseService periodClose)                 // 🔸 NEW
        {
            _repository = repository;
            _periodClose = periodClose;
        }

        public async Task<IEnumerable<PurchaseGoodReceiptItemsDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(PurchaseGoodReceiptItems goodReceiptItemsDTO)
        {
            // 🔒 period lock check based on GRN ReceptionDate
            await _periodClose.EnsureOpenAsync(goodReceiptItemsDTO.ReceptionDate);

            return await _repository.CreateAsync(goodReceiptItemsDTO);
        }

        public async Task<PurchaseGoodReceiptItemsDTO> GetById(long id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<IEnumerable<PurchaseGoodReceiptItemsViewInfo>> GetAllGRNDetailsAsync()
        {
            return await _repository.GetAllDetailsAsync();
        }

        public async Task UpdateAsync(PurchaseGoodReceiptItems purchaseGoodReceipt)
        {
            // 🔒 also protect update (if date changes or backdated changes not allowed)
            await _periodClose.EnsureOpenAsync(purchaseGoodReceipt.ReceptionDate);

            await _repository.UpdateAsync(purchaseGoodReceipt);
        }

        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }

        public async Task<IEnumerable<PurchaseGoodReceiptItemsDTO>> GetAllGRNByPoId()
        {
            return await _repository.GetAllGRNByPoId();
        }

        public async Task<ApplyGrnResult> ApplyGrnAndUpdateSalesOrderAsync(ApplyGrnAndSalesOrderRequest request)
        {
            if (request == null)
                throw new ArgumentNullException(nameof(request), "Request cannot be null.");

            if (string.IsNullOrWhiteSpace(request.ItemCode))
                throw new ArgumentException("ItemCode cannot be empty.");

            // repository already handles "no SO lines" as valid
            var result = await _repository.ApplyGrnAndUpdateSalesOrderAsync(
                request.ItemCode,
                request.WarehouseId,
                request.SupplierId,
                request.BinId,
                request.ReceivedQty
            );

            return result;
        }

    }
}
