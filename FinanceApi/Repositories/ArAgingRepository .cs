using FinanceApi.Data;
using FinanceApi.ModelDTO;
using Dapper;
using System.Data;
using FinanceApi.Repositories;


public class ArAgingRepository : DynamicRepository, IArAgingRepository
{
    public ArAgingRepository(IDbConnectionFactory connectionFactory)
        : base(connectionFactory)
    {
    }

    public async Task<IEnumerable<ArAgingSummaryDto>> GetSummaryAsync(DateTime fromDate, DateTime toDate)
    {
        var param = new { FromDate = fromDate, ToDate = toDate };
        return await Connection.QueryAsync<ArAgingSummaryDto>(
            "sp_ArAgingSummary",
            param,
            commandType: CommandType.StoredProcedure);
    }

    public async Task<IEnumerable<ArAgingInvoiceDto>> GetCustomerInvoicesAsync(
        int customerId,
        DateTime fromDate,
        DateTime toDate)
    {
        const string sql = @"
WITH Base AS (
    SELECT
        b.*,
        AgeRaw = DATEDIFF(DAY, b.InvoiceDate, @ToDate)
    FROM dbo.vwArAgingBase b
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
    CustomerId,
    CustomerName,
    OriginalAmount,
    PaidAmount,
    CreditAmount,
    Balance
FROM Norm
WHERE CustomerId = @CustomerId
ORDER BY InvoiceDate;
";

        return await Connection.QueryAsync<ArAgingInvoiceDto>(
            sql,
            new { CustomerId = customerId, FromDate = fromDate, ToDate = toDate });
    }
}
