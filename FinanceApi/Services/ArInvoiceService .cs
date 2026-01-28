using FinanceApi.ModelDTO;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace UnityWorksERP.Finance.AR
{
    public class ArInvoiceService : IArInvoiceService
    {
        private readonly IArInvoiceRepository _repo;

        public ArInvoiceService(IArInvoiceRepository repo)
        {
            _repo = repo;
        }

        public Task<int> CreateAdvanceAsync(ArAdvanceDto dto)
        {
           return _repo.CreateAdvanceAsync(dto);
        }

        public Task<IEnumerable<ArInvoiceListDto>> GetAllAsync()
            => _repo.GetAllAsync();

        public Task<IEnumerable<ArOpenAdvanceDto>> GetOpenAdvancesAsync(int customerId, int? salesOrderId)
        {
           return _repo.GetOpenAdvancesAsync(customerId, salesOrderId);
        }
    }
}
