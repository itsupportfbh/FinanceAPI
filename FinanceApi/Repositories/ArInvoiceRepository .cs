using Dapper;
using FinanceApi.Data;
using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Connections;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using UnityWorksERP.Finance.AR;

namespace FinanceApi.Repositories
{
    public class ArInvoiceRepository : DynamicRepository, IArInvoiceRepository
    {
        public ArInvoiceRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }
        public async Task<IEnumerable<ArInvoiceListDto>> GetAllAsync()
        {
            const string sql = @"
;WITH InvBase AS (
    SELECT
        v.Id          AS InvoiceId,
        v.InvoiceNo,
        v.InvoiceDate,
        v.Amount,
        v.PaidAmount,
        v.Balance,
        v.CustomerId,
        ISNULL(c.CustomerName,'') AS CustomerName
    FROM dbo.vwSalesInvoiceOpenForReceipt v
    LEFT JOIN dbo.Customer c ON c.Id = v.CustomerId
),

-- Credit per invoice (for that invoice's Outstanding)
CnInvoiceAgg AS (
    SELECT
        cn.SiId AS InvoiceId,
        InvoiceCreditAmount = SUM(ISNULL(cn.Subtotal,0))
    FROM dbo.CreditNote cn
    WHERE cn.IsActive = 1
      AND cn.SiId IS NOT NULL
    GROUP BY cn.SiId
),

-- Normal invoice rows
InvRows AS (
    SELECT
        RowType   = 'INV',
        i.InvoiceId,
        i.InvoiceNo,
        i.InvoiceDate,
        DueDate   = i.InvoiceDate,
        i.CustomerId,
        i.CustomerName,
        Amount     = i.Amount,
        Paid       = i.PaidAmount,
        CreditNote = ISNULL(ci.InvoiceCreditAmount,0),
        Outstanding = i.Amount - i.PaidAmount - ISNULL(ci.InvoiceCreditAmount,0),

        CustomerCreditNoteAmount = CAST(0 AS decimal(18,2)),
        CustomerCreditNoteNo     = NULL,
        CustomerCreditNoteDate   = NULL,
        CustomerCreditStatus     = 0,

        ReferenceNo = NULL          -- 🔹 no reference for normal invoice row
    FROM InvBase i
    LEFT JOIN CnInvoiceAgg  ci ON ci.InvoiceId = i.InvoiceId
),

-- ONE ROW PER CREDIT NOTE
CnRows AS (
    SELECT
        RowType   = 'CN',
        InvoiceId = ISNULL(cn.SiId, 0),        -- link CN to its Sales Invoice
        InvoiceNo = cn.CreditNoteNo,           -- CN number
        InvoiceDate = cn.CreditNoteDate,
        DueDate   = NULL,
        cn.CustomerId,
        ISNULL(c.CustomerName,'') AS CustomerName,

        Amount     = -cn.Subtotal,                               -- negative
        Paid       = CASE WHEN cn.Status > 1 THEN cn.Subtotal
                          ELSE 0 END,
        CreditNote = CAST(0 AS decimal(18,2)),
        Outstanding = CASE WHEN cn.Status > 1 THEN 0
                           ELSE -cn.Subtotal END,

        CustomerCreditNoteAmount = cn.Subtotal,
        CustomerCreditNoteNo     = cn.CreditNoteNo,
        CustomerCreditNoteDate   = cn.CreditNoteDate,
        CustomerCreditStatus     = cn.Status,

        ReferenceNo = si.InvoiceNo   -- 🔹 Sales Invoice No this CN is applied to
    FROM dbo.CreditNote cn
    LEFT JOIN dbo.Customer    c  ON c.Id  = cn.CustomerId
    LEFT JOIN dbo.SalesInvoice si ON si.Id = cn.SiId
    WHERE cn.IsActive = 1
)

SELECT
    RowType,
    InvoiceId,
    InvoiceNo,
    InvoiceDate,
    DueDate,
    CustomerId,
    CustomerName,
    Amount,
    Paid,
    CreditNote,
    Outstanding,
    CustomerCreditNoteAmount,
    CustomerCreditNoteNo,
    CustomerCreditNoteDate,
    CustomerCreditStatus,
    ReferenceNo
FROM InvRows

UNION ALL

SELECT
    RowType,
    InvoiceId,
    InvoiceNo,
    InvoiceDate,
    DueDate,
    CustomerId,
    CustomerName,
    Amount,
    Paid,
    CreditNote,
    Outstanding,
    CustomerCreditNoteAmount,
    CustomerCreditNoteNo,
    CustomerCreditNoteDate,
    CustomerCreditStatus,
    ReferenceNo
FROM CnRows

ORDER BY CustomerName, InvoiceDate, InvoiceNo;
";

            return await Connection.QueryAsync<ArInvoiceListDto>(sql);
        }

        public async Task<int> CreateAdvanceAsync(ArAdvanceDto dto)
        {
            const string sql = @"
DECLARE @AdvanceId INT;
DECLARE @BankHeadId INT;
DECLARE @CustomerHeadId INT;

-- 1) Get Bank GL Head
SELECT @BankHeadId = b.BudgetLineId
FROM Bank b
WHERE b.Id = @BankAccountId;

-- 2) (Optional) Get Customer GL Head – used only for GL, not AccountBalance
SELECT @CustomerHeadId = c.BudgetLineId
FROM Customer c
WHERE c.Id = @CustomerId;

-- 3) Insert Advance Master
INSERT INTO ArCustomerAdvance
(
    CustomerId,
    SalesOrderId,
    AdvanceNo,
    AdvanceDate,
    Amount,
    BalanceAmount,
    PaymentMode,
    BankAccountId,
    Remarks
)
VALUES
(
    @CustomerId,
    @SalesOrderId,
    CONCAT('ADV-', FORMAT(GETDATE(),'yyyyMMddHHmmss')),
    @AdvanceDate,
    @Amount,
    @Amount,
    @PaymentMode,
    @BankAccountId,
    @Remarks
);

SET @AdvanceId = SCOPE_IDENTITY();

-------------------------------------------------
-- ✅ ONLY BANK GOES TO AccountBalance
-------------------------------------------------
IF EXISTS (SELECT 1 FROM AccountBalance WHERE HeadId = @BankHeadId)
BEGIN
    UPDATE AccountBalance
       SET PeriodDebit      = ISNULL(PeriodDebit,0) + @Amount,
           AvailableBalance = (ISNULL(PeriodDebit,0) + @Amount)
                              - ISNULL(PeriodCredit,0),
           UpdatedDate      = GETDATE()
     WHERE HeadId = @BankHeadId;
END
ELSE
BEGIN
    INSERT INTO AccountBalance
        (HeadId, PeriodDebit, PeriodCredit, AvailableBalance, UpdatedDate)
    VALUES
        (@BankHeadId, @Amount, 0, @Amount, GETDATE());
END

-- ❌ NO ROW FOR CUSTOMER HERE

SELECT @AdvanceId;
";

            return await Connection.ExecuteScalarAsync<int>(sql, dto);
        }


        public async Task<IEnumerable<ArOpenAdvanceDto>> GetOpenAdvancesAsync(int customerId, int? salesOrderId)
        {
            const string sql = "sp_ArCustomerOpenAdvances";

           
            return await Connection.QueryAsync<ArOpenAdvanceDto>(
                sql,
                new { CustomerId = customerId, SalesOrderId = salesOrderId },
                commandType: CommandType.StoredProcedure
            );
        }
       

    }
}
