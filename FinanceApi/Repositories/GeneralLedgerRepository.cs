using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FinanceApi.Repositories
{
    public class GeneralLedgerRepository : DynamicRepository, IGeneralLedgerRepository
    {
        public GeneralLedgerRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<GeneralLedgerDTO>> GetAllAsync()
        {
            const string query = @"
WITH
------------------------------------------------------------
-- A) SAFE COA TREE (ParentHead = parent HEADCODE)
------------------------------------------------------------
CoaCTE AS (
    -- ROOT (no parent)
    SELECT
        c.Id,
        c.HeadCode,
        c.HeadName,
        c.HeadType,
        c.ParentHead,
        RootHeadId   = c.Id,
        RootHeadType =
            CASE c.HeadType
                WHEN 'A' THEN 'ASSET'
                WHEN 'L' THEN 'LIABILITY'
                WHEN 'E' THEN 'EQUITY'
                WHEN 'I' THEN 'INCOME'
                WHEN 'X' THEN 'EXPENSE'
                ELSE c.HeadType
            END,
        Level = 0
    FROM dbo.ChartOfAccount c
    WHERE (c.ParentHead IS NULL OR c.ParentHead = 0)
      AND c.IsActive = 1

    UNION ALL

    -- CHILDREN
    SELECT
        c.Id,
        c.HeadCode,
        c.HeadName,
        c.HeadType,
        c.ParentHead,
        cr.RootHeadId,
        cr.RootHeadType,
        Level = cr.Level + 1
    FROM dbo.ChartOfAccount c
    INNER JOIN CoaCTE cr
        ON c.ParentHead = cr.HeadCode    -- ParentHead stores parent HEADCODE
    WHERE c.IsActive = 1
      AND cr.Level < 20
),
CoaRoot AS (
    SELECT
        Id,
        HeadCode,
        HeadName,
        HeadType,
        ParentHead,
        RootHeadId,
        RootHeadType
    FROM (
        SELECT
            cte.*,
            ROW_NUMBER() OVER (PARTITION BY cte.Id ORDER BY cte.Level) AS rn
        FROM CoaCTE cte
    ) x
    WHERE x.rn = 1
),

------------------------------------------------------------
-- B) GL LINES (all modules)
------------------------------------------------------------
GlLines AS (

    --------------------------------------------------------
    -- 1 ► Opening Balance  (only active rows)
    --------------------------------------------------------
    SELECT
        ob.BudgetLineId AS HeadId,
        OpeningBalance = ISNULL(ob.OpeningBalanceAmount, 0),
        Debit  = 0,
        Credit = 0
    FROM dbo.OpeningBalance ob
    WHERE ob.IsActive = 1

    UNION ALL

    --------------------------------------------------------
    -- 2 ► Manual Journal (original side – BudgetLineId)
    --------------------------------------------------------
    SELECT
        mjl.AccountId      AS HeadId,         -- = ChartOfAccount.Id
        OpeningBalance = 0,
        Debit          = ISNULL(mjl.Debit,  0),
        Credit         = ISNULL(mjl.Credit, 0)
    FROM dbo.ManualJournal      mj
    INNER JOIN dbo.ManualJournalLine mjl
        ON mjl.JournalId = mj.Id
    WHERE mj.IsActive  = 1
      AND mj.IsPosted  = 1
      AND mjl.IsActive = 1

    UNION ALL

    --------------------------------------------------------
    -- 3 ► Sales Invoice – DR Customer, CR Income/Asset
    --------------------------------------------------------

    -- 3a) DR Customer via SalesOrder (SoId on SI)
    SELECT
        coaCust.Id AS HeadId,
        0,
        Debit  = ISNULL(sil.LineAmount, 0),
        0
    FROM dbo.SalesInvoiceLine sil
    INNER JOIN dbo.SalesInvoice si
        ON si.Id = sil.SiId
       AND si.SoId IS NOT NULL
    INNER JOIN dbo.SalesOrder so
        ON so.Id = si.SoId
    INNER JOIN dbo.Customer cs
        ON cs.Id = so.CustomerId
    INNER JOIN dbo.ChartOfAccount coaCust
        ON coaCust.Id = cs.BudgetLineId

    UNION ALL

    -- 3b) DR Customer via DeliveryOrderLine when DoId is set
    SELECT
        coaCust.Id AS HeadId,
        0,
        Debit  = ISNULL(sil.LineAmount, 0),
        0
    FROM dbo.SalesInvoiceLine sil
    INNER JOIN dbo.SalesInvoice si
        ON si.Id = sil.SiId
       AND si.SoId IS NULL
       AND si.DoId IS NOT NULL
    INNER JOIN dbo.DeliveryOrderLine dol
        ON dol.DoId = si.DoId
    INNER JOIN dbo.SalesOrderLines sol
        ON sol.Id = dol.SoLineId
    INNER JOIN dbo.SalesOrder so
        ON so.Id = sol.SalesOrderId
    INNER JOIN dbo.Customer cs
        ON cs.Id = so.CustomerId
    INNER JOIN dbo.ChartOfAccount coaCust
        ON coaCust.Id = cs.BudgetLineId

    UNION ALL

    -- 3c) CR Income / Asset (item budget line)
    SELECT
        sil.BudgetLineId AS HeadId,
        0,
        0,
        Credit = ISNULL(sil.LineAmount, 0)
    FROM dbo.SalesInvoiceLine sil

    UNION ALL

    --------------------------------------------------------
    -- 4 ► Supplier Invoice PIN – DR Expense/Asset, CR Supplier
    --------------------------------------------------------
    -- 4a) CR Supplier (AP +)
    SELECT
        sps.BudgetLineId AS HeadId,
        0,
        0,
        Credit = ISNULL(sip.Amount, 0)
    FROM dbo.SupplierInvoicePin sip
    INNER JOIN dbo.Suppliers sps ON sps.Id = sip.SupplierId
    WHERE sip.IsActive = 1

    UNION ALL

    -- 4b) DR Expense / Asset (PIN lines from JSON)
    SELECT
        L.BudgetLineId AS HeadId,
        0,
        Debit = ISNULL(sip.Amount, 0),
        0
    FROM dbo.SupplierInvoicePin sip
    CROSS APPLY OPENJSON(sip.LinesJson)
    WITH (
        BudgetLineId INT '$.budgetLineId'
    ) AS L
    WHERE sip.IsActive = 1

    UNION ALL

    --------------------------------------------------------
    -- 5 ► AR Receipt – DR Bank / Cash, CR Customer
    --------------------------------------------------------
    -- 5a) DR BANK  (BankId NOT NULL)
    SELECT
        bk.BudgetLineId AS HeadId,
        0,
        Debit  = ISNULL(ar.TotalAllocated, 0),
        0
    FROM dbo.ArReceipt ar
    INNER JOIN dbo.Bank bk ON bk.Id = ar.BankId
    WHERE ar.IsActive = 1
      AND ar.BankId IS NOT NULL

    UNION ALL

    -- 5b) DR CASH-IN-HAND  (BankId NULL or 0)
    SELECT
        cashCoa.Id AS HeadId,
        0,
        Debit = ISNULL(ar.TotalAllocated, 0),
        0
    FROM dbo.ArReceipt ar
    CROSS JOIN dbo.ChartOfAccount cashCoa
    WHERE ar.IsActive = 1
      AND (ar.BankId IS NULL OR ar.BankId = 0)
      AND cashCoa.IsActive = 1
      AND cashCoa.HeadName = 'Cash'

    UNION ALL

    -- 5c) Customer – AR Receipt as negative Debit (net AR)
    SELECT
        coaCust.Id AS HeadId,
        0,
        Debit  = -ISNULL(ra.AllocatedAmount, 0),  -- receipt = minus debit
        0
    FROM dbo.ArReceiptAllocation ra
    INNER JOIN dbo.ArReceipt    ar ON ar.Id = ra.ReceiptId
    INNER JOIN dbo.SalesInvoice si ON si.Id = ra.InvoiceId
    INNER JOIN dbo.SalesOrder  so ON so.Id = si.SoId
    INNER JOIN dbo.Customer    cs ON cs.Id = so.CustomerId
    INNER JOIN dbo.ChartOfAccount coaCust
        ON coaCust.Id = cs.BudgetLineId
    WHERE ra.IsActive = 1

    UNION ALL

    --------------------------------------------------------
    -- 6 ► Supplier Payment – DR Supplier, CR Bank / Cash
    --------------------------------------------------------
    -- 6a) Supplier – Payment as negative Credit (net AP)
    SELECT
        sps.BudgetLineId AS HeadId,
        0,
        0,
        Credit = -ISNULL(sp.Amount, 0)     -- payment = minus credit
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Suppliers sps ON sps.Id = sp.SupplierId
    WHERE sp.IsActive = 1

    UNION ALL

    -- 6b) CR Bank (NON-CASH payments)
    SELECT
        bk.BudgetLineId AS HeadId,
        0,
        0,
        Credit = ISNULL(sp.Amount, 0)
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Bank bk ON bk.Id = sp.BankId
    WHERE sp.IsActive = 1
      AND sp.PaymentMethodId <> 1     -- not cash

    UNION ALL

    -- 6c) CR Cash (CASH payments)
    SELECT
        cashCoa.Id AS HeadId,
        0,
        0,
        Credit = ISNULL(sp.Amount, 0)
    FROM dbo.SupplierPayment sp
    CROSS JOIN dbo.ChartOfAccount cashCoa
    WHERE sp.IsActive = 1
      AND sp.PaymentMethodId = 1      -- CASH
      AND cashCoa.IsActive = 1
      AND cashCoa.HeadName = 'Cash'

    UNION ALL

    --------------------------------------------------------
    -- 7 ► Supplier Debit Note – DR Supplier, CR Expense/Income
    --------------------------------------------------------
    -- DR Supplier (AP +)
    SELECT
        sps.BudgetLineId AS HeadId,
        0,
        Debit = ISNULL(sdn.Amount, 0),
        0
    FROM dbo.SupplierDebitNote sdn
    INNER JOIN dbo.Suppliers sps ON sps.Id = sdn.SupplierId
    WHERE sdn.IsActive = 1

    UNION ALL

    -- CR Expense / Income (lines)
    SELECT
        L.BudgetLineId AS HeadId,
        0,
        0,
        Credit = ISNULL(L.Amount, 0)
    FROM dbo.SupplierDebitNote sdn
    CROSS APPLY OPENJSON(sdn.LinesJson)
    WITH (
        BudgetLineId INT            '$.budgetLineId',
        Amount       DECIMAL(18,2)  '$.amount'
    ) AS L
    WHERE sdn.IsActive = 1

    UNION ALL

    --------------------------------------------------------
    -- 8 ► Customer Credit Note – DR Income, CR Customer
    --------------------------------------------------------
    -- DR Income (item’s account)
    SELECT
        siLine.BudgetLineId AS HeadId,
        0,
        Debit = ISNULL(cn.Subtotal, 0),
        0
    FROM dbo.CreditNote cn
    INNER JOIN dbo.SalesInvoice     si     ON si.Id = cn.SiId
    INNER JOIN dbo.SalesInvoiceLine siLine ON siLine.SiId = si.Id
    WHERE cn.IsActive = 1

    UNION ALL

    -- CR Customer (AR −)
    SELECT
        coaCust.Id AS HeadId,
        0,
        0,
        Credit = ISNULL(cn.Subtotal, 0)
    FROM dbo.CreditNote cn
    INNER JOIN dbo.Customer cs ON cs.Id = cn.CustomerId
    INNER JOIN dbo.ChartOfAccount coaCust
        ON coaCust.Id = cs.BudgetLineId
    WHERE cn.IsActive = 1

    UNION ALL

    --------------------------------------------------------
    -- 9 ► CUSTOMER ADVANCE – CR Advance Payment
    --------------------------------------------------------
    SELECT
        advCoa.Id AS HeadId,
        0,
        0,
        Credit = ISNULL(ca.BalanceAmount, 0)
    FROM dbo.ArCustomerAdvance ca
    CROSS JOIN dbo.ChartOfAccount advCoa
    WHERE ca.IsActive = 1
      AND ISNULL(ca.BalanceAmount, 0) <> 0
      AND advCoa.IsActive = 1
      AND (
            advCoa.HeadName = 'ADVANCE PAYMENT'
         OR advCoa.HeadCode = 1010402
      )

    UNION ALL

    --------------------------------------------------------
    -- 10 ► SUPPLIER ADVANCE – DR Advance Payment
    --------------------------------------------------------
    SELECT
        advCoa.Id AS HeadId,
        0,
        Debit  = ISNULL(sa.BalanceAmount, 0),
        Credit = 0
    FROM dbo.SupplierAdvance sa
    CROSS JOIN dbo.ChartOfAccount advCoa
    WHERE sa.IsActive = 1
      AND ISNULL(sa.BalanceAmount, 0) <> 0
      AND advCoa.IsActive = 1
      AND (
            advCoa.HeadName = 'ADVANCE PAYMENT'
         OR advCoa.HeadCode = 1010402
      )
),

------------------------------------------------------------
-- C) Aggregate per Head
------------------------------------------------------------
GlAgg AS (
    SELECT
        HeadId,
        OpeningBalance = SUM(ISNULL(OpeningBalance, 0)),
        Debit          = SUM(ISNULL(Debit, 0)),
        Credit         = SUM(ISNULL(Credit, 0)),
        Balance        = SUM(ISNULL(OpeningBalance, 0))
                        + SUM(ISNULL(Debit, 0))
                        - SUM(ISNULL(Credit, 0))
    FROM GlLines
    GROUP BY HeadId
),

------------------------------------------------------------
-- D) Main COA rows + Item rows
------------------------------------------------------------
FinalRows AS (

    -- Original COA heads
    SELECT
        c.Id         AS HeadId,
        c.HeadCode,
        c.HeadName,
        c.ParentHead,
        c.HeadType,
        cr.RootHeadType,

        -- clamp negatives to 0
        OpeningBalance =
            CASE WHEN ISNULL(ga.OpeningBalance, 0) < 0
                 THEN 0
                 ELSE ISNULL(ga.OpeningBalance, 0)
            END,
        Debit =
            CASE WHEN ISNULL(ga.Debit, 0) < 0
                 THEN 0
                 ELSE ISNULL(ga.Debit, 0)
            END,
        Credit =
            CASE WHEN ISNULL(ga.Credit, 0) < 0
                 THEN 0
                 ELSE ISNULL(ga.Credit, 0)
            END,
        Balance =
            CASE WHEN ISNULL(ga.Balance, 0) < 0
                 THEN 0
                 ELSE ISNULL(ga.Balance, 0)
            END,
        c.IsTransaction AS IsControl
    FROM dbo.ChartOfAccount c
    LEFT JOIN CoaRoot cr ON cr.Id = c.Id
    LEFT JOIN GlAgg   ga ON ga.HeadId = c.Id
    WHERE c.IsActive = 1
	)

------------------------------------------------------------
-- E) Final result
------------------------------------------------------------
SELECT
    HeadId,
    HeadCode,
    HeadName,
    ParentHead,
    HeadType,
    RootHeadType,
    OpeningBalance,
    Debit,
    Credit,
    Balance,
    IsControl
FROM FinalRows
ORDER BY HeadCode;

";

            return await Connection.QueryAsync<GeneralLedgerDTO>(query);
        }





    }
}
