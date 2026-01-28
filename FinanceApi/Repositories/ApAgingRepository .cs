using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using FinanceApi.Data;
using FinanceApi.ModelDTO;

namespace FinanceApi.Repositories
{
    public class ApAgingRepository : DynamicRepository, IApAgingRepository
    {
        public ApAgingRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        /// <summary>
        /// Summary by supplier (0-30, 31-60, 61-90, 90+ buckets).
        /// Uses stored procedure: dbo.sp_ApAgingSummary
        /// </summary>
        public async Task<IEnumerable<ApAgingSummaryDto>> GetSummaryAsync(
            DateTime fromDate,
            DateTime toDate)
        {
            var param = new { FromDate = fromDate, ToDate = toDate };

            return await Connection.QueryAsync<ApAgingSummaryDto>(
                "sp_ApAgingSummary",
                param,
                commandType: CommandType.StoredProcedure);
        }

        /// <summary>
        /// Detailed invoice list for a specific supplier.
        /// Uses vwApAgingBase (same concept as vwArAgingBase).
        /// </summary>
        public async Task<IEnumerable<ApAgingInvoiceDto>> GetSupplierInvoicesAsync(
            int supplierId,
            DateTime fromDate,
            DateTime toDate)
        {
            const string sql = @"
;WITH Base AS (
    SELECT
        b.*,
        AgeRaw = DATEDIFF(DAY, b.InvoiceDate, @ToDate)
    FROM dbo.vwApAgingBase b
    WHERE b.InvoiceDate >= @FromDate
      AND b.InvoiceDate <= @ToDate
),
Norm AS (
    SELECT
        *,
        AgeDays =
            CASE WHEN AgeRaw < 0 THEN 0 ELSE AgeRaw END,
        BucketName =
            CASE 
                WHEN AgeRaw < 0               THEN '0-30'
                WHEN AgeRaw BETWEEN 0  AND 30 THEN '0-30'
                WHEN AgeRaw BETWEEN 31 AND 60 THEN '31-60'
                WHEN AgeRaw BETWEEN 61 AND 90 THEN '61-90'
                ELSE '90+'
            END
    FROM Base
)
SELECT
    InvoiceId,
    InvoiceNo,
    InvoiceDate,
    DueDate,
    AgeDays,
    BucketName,
    SupplierId,
    SupplierName,
    OriginalAmount,
    PaidAmount,
    CreditAmount,
    Balance
FROM Norm
WHERE SupplierId = @SupplierId
ORDER BY InvoiceDate;";

            var param = new
            {
                SupplierId = supplierId,
                FromDate = fromDate,
                ToDate = toDate
            };

            return await Connection.QueryAsync<ApAgingInvoiceDto>(sql, param);
        }
    }
}
