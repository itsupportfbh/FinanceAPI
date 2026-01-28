using FinanceApi.InterfaceService;
using System.Linq;
using System.Threading.Tasks;

namespace UnityWorksERP.Finance.AR
{
    public class ArReceiptService : IArReceiptService
    {
        private readonly IArReceiptRepository _repo;
        private readonly IPeriodCloseService _periodClose;   // 🔸 NEW

        public ArReceiptService(
            IArReceiptRepository repo,
            IPeriodCloseService periodClose)                 // 🔸 NEW
        {
            _repo = repo;
            _periodClose = periodClose;
        }

        public Task<IEnumerable<ArReceiptListDto>> GetListAsync()
            => _repo.GetListAsync();

        public Task<ArReceiptDetailDto?> GetByIdAsync(int id)
            => _repo.GetByIdAsync(id);

        public Task<IEnumerable<SalesInvoiceOpenDto>> GetOpenInvoicesAsync(int customerId)
            => _repo.GetOpenInvoicesAsync(customerId);

        public async Task<int> CreateAsync(ArReceiptCreateUpdateDto dto, int userId)
        {
            Validate(dto);

            // 🔒 Period lock check (based on receipt date)
            await _periodClose.EnsureOpenAsync(dto.ReceiptDate);

            return await _repo.CreateAsync(dto, userId);
        }

        public async Task UpdateAsync(ArReceiptCreateUpdateDto dto, int userId)
        {
            if (!dto.Id.HasValue)
                throw new InvalidOperationException("Id is required for update.");

            Validate(dto);

            // 🔒 Period lock check (based on receipt date)
            await _periodClose.EnsureOpenAsync(dto.ReceiptDate);

            await _repo.UpdateAsync(dto, userId);
        }

        public Task DeleteAsync(int id, int userId)
            => _repo.DeleteAsync(id, userId);

        private static void Validate(ArReceiptCreateUpdateDto dto)
        {
            if (dto.CustomerId <= 0)
                throw new InvalidOperationException("Customer is required.");

            if (dto.AmountReceived <= 0)
                throw new InvalidOperationException("Amount received must be > 0.");

            if (dto.Allocations == null || dto.Allocations.Count == 0)
                throw new InvalidOperationException("At least one allocation is required.");

            var totalAllocated = dto.Allocations.Sum(a => a.AllocatedAmount);

            if (totalAllocated <= 0)
                throw new InvalidOperationException("Allocated amount must be > 0.");

            if (totalAllocated > dto.AmountReceived)
                throw new InvalidOperationException("Allocated amount cannot be more than amount received.");
        }
    }
}
