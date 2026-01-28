using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using UnityWorksERP.Finance.AR;

namespace FinanceApi.Interfaces
{
    public interface IArCollectionForecastRepository
    {
        Task<IEnumerable<ArCollectionForecastSummaryDto>> GetSummaryAsync(
            DateTime? fromDate,
            DateTime? toDate);

        Task<IEnumerable<ArCollectionForecastDetailDto>> GetDetailAsync(
            int customerId,
            DateTime? fromDate,
            DateTime? toDate);
    }
}

