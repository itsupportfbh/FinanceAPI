// Services/AccountsPayableService.cs
using System.Collections.Generic;
using System.Threading.Tasks;
using FinanceApi.InterfaceService;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;

namespace FinanceApi.Services
{
    public class AccountsPayableService : IAccountsPayableService
    {
        private readonly IAccountsPayableRepository _repo;
        private readonly IPeriodCloseService _periodLock;

        public AccountsPayableService(
            IAccountsPayableRepository repo,
            IPeriodCloseService periodLock)
        {
            _repo = repo;
            _periodLock = periodLock;
        }

        public Task<IEnumerable<ApInvoiceDTO>> GetApInvoicesAsync()
            => _repo.GetApInvoicesAsync();

        public Task<IEnumerable<ApInvoiceDTO>> GetApInvoicesBySupplierAsync(int supplierId)
            => _repo.GetApInvoicesBySupplierAsync(supplierId);

        public Task<IEnumerable<ApMatchDTO>> GetMatchListAsync()
            => _repo.GetMatchListAsync();

        public Task<IEnumerable<ApPaymentListDto>> GetPaymentsAsync()
            => _repo.GetPaymentsAsync();

        public async Task<int> CreatePaymentAsync(ApPaymentCreateDto dto, int userId)
        {
            // 🔒 Period lock check
            await _periodLock.EnsureOpenAsync(dto.PaymentDate);

            // ✅ Insert payment
            return await _repo.CreatePaymentAsync(dto, userId);
        }
        public Task<IEnumerable<BankAccountDTO>> GetAllAsync()
        {
            return _repo.GetAllAsync();
        }

        public Task<BankAccountDTO?> GetByIdAsync(int bankId)
        {
            return _repo.GetByIdAsync(bankId);
        }
        public async Task<int> UpdateBankBalance(int bankId, decimal newBal)
        {
            return await _repo.UpdateBankBalance(bankId, newBal);
        }

        public async Task<IEnumerable<ApSupplierAdvanceDto>> GetSupplierAdvancesAsync()
        {
            return await _repo.GetSupplierAdvancesAsync();
        }

        public async Task<int> CreateSupplierAdvanceAsync(int userId, ApSupplierAdvanceCreateRequest req)
        {
            return await _repo.CreateSupplierAdvanceAsync(userId, req);
        }

        public async Task<IEnumerable<object>> GetSupplierAdvancesAsync(int supplierId)
        {
            return await _repo.GetSupplierAdvancesAsync(supplierId);  
        }

        public async Task<IEnumerable<SupplierAdvanceListRowDto>> GetSupplierAdvancesListAsync()
        {
            return await _repo.GetSupplierAdvancesListAsync();
        }
        public Task<IEnumerable<ArAdvanceListDto>> GetAdvanceListAsync()
        {
            return _repo.GetAdvanceListAsync();
        }
    }
}
