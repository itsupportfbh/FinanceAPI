using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ISalesReportRepository
    {
        Task<IEnumerable<SalesReportDTO>> GetSalesByItemAsync();
        Task<IEnumerable<SalesMarginReportViewInfo>> GetSalesMarginAsync();
    }
}
