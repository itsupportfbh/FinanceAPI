using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FinanceApi.ModelDTO;

namespace FinanceApi.Repositories
{
    public interface IApAgingRepository
    {
        Task<IEnumerable<ApAgingSummaryDto>> GetSummaryAsync(DateTime fromDate, DateTime toDate);

        Task<IEnumerable<ApAgingInvoiceDto>> GetSupplierInvoicesAsync(
            int supplierId,
            DateTime fromDate,
            DateTime toDate);
    }
}
