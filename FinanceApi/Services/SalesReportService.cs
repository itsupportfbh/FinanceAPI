using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;

namespace FinanceApi.Services
{
    public class SalesReportService : ISalesReportService
    {
        private readonly ISalesReportRepository _salesReportRepository;

        public SalesReportService(ISalesReportRepository salesReportRepository)
        {
            _salesReportRepository = salesReportRepository;
        }


        public async Task<IEnumerable<SalesReportDTO>> GetSalesByItemAsync()
        {
            return await _salesReportRepository.GetSalesByItemAsync();
        }

        public async Task<IEnumerable<SalesMarginReportViewInfo>> GetSalesMarginAsync()
        {
            return await _salesReportRepository.GetSalesMarginAsync();
        }
    }
}
