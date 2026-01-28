using FinanceApi.ModelDTO;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace UnityWorksERP.Finance.AR
{
    public interface IArInvoiceRepository
    {
        Task<IEnumerable<ArInvoiceListDto>> GetAllAsync();
        Task<int> CreateAdvanceAsync(ArAdvanceDto dto);
        Task<IEnumerable<ArOpenAdvanceDto>> GetOpenAdvancesAsync(int customerId, int? salesOrderId);
    }
}
