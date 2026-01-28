using FinanceApi.ModelDTO;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace UnityWorksERP.Finance.AR
{
    public interface IArInvoiceService
    {
        Task<IEnumerable<ArInvoiceListDto>> GetAllAsync();
        Task<int> CreateAdvanceAsync(ArAdvanceDto dto);
        Task<IEnumerable<ArOpenAdvanceDto>> GetOpenAdvancesAsync(int customerId, int? salesOrderId);
    }
}
