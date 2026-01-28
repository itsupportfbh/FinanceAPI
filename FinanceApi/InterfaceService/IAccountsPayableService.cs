// InterfaceService/IAccountsPayableService.cs
using System.Collections.Generic;
using System.Threading.Tasks;
using FinanceApi.ModelDTO;

namespace FinanceApi.InterfaceService
{
    public interface IAccountsPayableService
    {
        Task<IEnumerable<ApInvoiceDTO>> GetApInvoicesAsync();
        Task<IEnumerable<ApInvoiceDTO>> GetApInvoicesBySupplierAsync(int supplierId);
        Task<IEnumerable<ApMatchDTO>> GetMatchListAsync();
        Task<IEnumerable<ApPaymentListDto>> GetPaymentsAsync();
        Task<int> CreatePaymentAsync(ApPaymentCreateDto dto, int userId);
        Task<IEnumerable<BankAccountDTO>> GetAllAsync();
        Task<BankAccountDTO?> GetByIdAsync(int bankId);
        Task<int> UpdateBankBalance(int bankId, decimal newBal);
        Task<IEnumerable<ApSupplierAdvanceDto>> GetSupplierAdvancesAsync();
        Task<int> CreateSupplierAdvanceAsync(int userId, ApSupplierAdvanceCreateRequest req);
        Task<IEnumerable<object>> GetSupplierAdvancesAsync(int supplierId);
        Task<IEnumerable<SupplierAdvanceListRowDto>> GetSupplierAdvancesListAsync();
        Task<IEnumerable<ArAdvanceListDto>> GetAdvanceListAsync();
    }
}
