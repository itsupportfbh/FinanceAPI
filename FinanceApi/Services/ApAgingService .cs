using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FinanceApi.Repositories;

public class ApAgingService : IApAgingService
{
    private readonly IApAgingRepository _apAgingRepository;

    public ApAgingService(IApAgingRepository apAgingRepository)
    {
        _apAgingRepository = apAgingRepository;
    }

    public Task<IEnumerable<ApAgingSummaryDto>> GetSummaryAsync(DateTime fromDate, DateTime toDate)
    {
        return _apAgingRepository.GetSummaryAsync(fromDate, toDate);
    }

    public Task<IEnumerable<ApAgingInvoiceDto>> GetSupplierInvoicesAsync(
        int supplierId,
        DateTime fromDate,
        DateTime toDate)
    {
        return _apAgingRepository.GetSupplierInvoicesAsync(supplierId, fromDate, toDate);
    }
}
