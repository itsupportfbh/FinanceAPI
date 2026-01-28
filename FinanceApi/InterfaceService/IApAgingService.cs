using System;
using System.Collections.Generic;
using System.Threading.Tasks;

public interface IApAgingService
{
    Task<IEnumerable<ApAgingSummaryDto>> GetSummaryAsync(DateTime fromDate, DateTime toDate);

    Task<IEnumerable<ApAgingInvoiceDto>> GetSupplierInvoicesAsync(
        int supplierId,
        DateTime fromDate,
        DateTime toDate);
}

