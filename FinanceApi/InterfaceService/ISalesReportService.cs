using FinanceApi.ModelDTO;

namespace FinanceApi.InterfaceService
{
    public interface ISalesReportService
    {
        Task<IEnumerable<SalesReportDTO>> GetSalesByItemAsync();

        Task<IEnumerable<SalesMarginReportViewInfo>> GetSalesMarginAsync();
    }
}
