// Interfaces/IAccountsPayableRepository.cs
using System.Collections.Generic;
using System.Threading.Tasks;
using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface IAccountsPayableRepository
    {
        Task<IEnumerable<ApInvoiceDTO>> GetApInvoicesAsync();
        Task<IEnumerable<ApInvoiceDTO>> GetApInvoicesBySupplierAsync(int supplierId);
        Task<IEnumerable<ApMatchDTO>> GetMatchListAsync();
        Task<int> CreatePaymentAsync(ApPaymentCreateDto dto, int userId);
        Task<IEnumerable<ApPaymentListDto>> GetPaymentsAsync();
        Task<IEnumerable<BankAccountDTO>> GetAllAsync();
        Task<BankAccountDTO?> GetByIdAsync(int bankId);
        Task<int> UpdateBankBalance(int bankId, decimal newBalance);
        Task<IEnumerable<ApSupplierAdvanceDto>> GetSupplierAdvancesAsync();
        Task<int> CreateSupplierAdvanceAsync(int userId, ApSupplierAdvanceCreateRequest req);

        // for dropdown in SI / AP payment screens
        Task<IEnumerable<object>> GetSupplierAdvancesAsync(int supplierId);
        Task<IEnumerable<SupplierAdvanceListRowDto>> GetSupplierAdvancesListAsync();
        Task<IEnumerable<ArAdvanceListDto>> GetAdvanceListAsync();
    }
}
