using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.ModelDTO.TB;

namespace FinanceApi.Repositories
{
    public class FinanceReportRepository : DynamicRepository, IFinanceReportRepository
    {
        public FinanceReportRepository(IDbConnectionFactory factory)
            : base(factory)
        {
        }
        // ============================================================
        //  TRIAL BALANCE SUMMARY  (Opening / Closing, Hierarchical)
        // ============================================================
        public async Task<IEnumerable<TrialBalanceDTO>> GetTrialBalanceAsync(ReportBaseDTO dto)
        {
            const string sql = @"
;WITH

------------------------------------------------------------
-- 0) CASH COA (Assets >> Current Assets >> Cash-in-hand >> Cash)
------------------------------------------------------------
CashCoa AS (
    SELECT TOP (1) Id AS CashHeadId
    FROM dbo.ChartOfAccount
    WHERE HeadName = 'Cash'   -- adjust name if different
      AND IsActive = 1
),

------------------------------------------------------------
-- 1) VIRTUAL GL LINES (ALL MODULES)
------------------------------------------------------------
GlLines AS (

     --------------------------------------------------------
    -- A) Manual Journal – single leg per line (all accounts)
    --------------------------------------------------------
    SELECT
        mj.JournalDate                          AS TransDate,
        'MJ'                                    AS SourceType,
        mj.JournalNo                            AS SourceNo,
        ISNULL(mjl.LineDescription, mj.Description) AS Description,
        mjl.AccountId                           AS HeadId,
        ISNULL(TRY_CONVERT(decimal(18,2), mjl.Debit), 0)  AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), mjl.Credit), 0) AS Credit
    FROM dbo.ManualJournal mj
    INNER JOIN dbo.ManualJournalLine mjl
        ON mjl.JournalId = mj.Id
       AND mjl.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = mjl.AccountId
       AND coa.IsActive = 1
    WHERE mj.IsActive = 1
      AND mj.IsPosted = 1
      AND mj.JournalDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                             AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- B) Sales Invoice lines  (P&L / revenue / expense)
    --    Line.BudgetLineId -> fallback Item.BudgetLineId
    --------------------------------------------------------
    SELECT
        si.InvoiceDate                        AS TransDate,
        'SI'                                  AS SourceType,
        si.InvoiceNo                          AS SourceNo,
        ISNULL(sil.Description,'')            AS Description,
        coa.Id                                AS HeadId,
        CAST(0 AS decimal(18,2))              AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), sil.LineAmount),0)
      + ISNULL(TRY_CONVERT(decimal(18,2), sil.Tax),0)      AS Credit
    FROM dbo.SalesInvoiceLine sil
    INNER JOIN dbo.SalesInvoice si
        ON si.Id = sil.SiId AND si.IsActive = 1
    INNER JOIN dbo.Item i
        ON i.Id = sil.ItemId AND i.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = COALESCE(sil.BudgetLineId, i.BudgetLineId)
       AND coa.IsActive = 1
    WHERE si.InvoiceDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                        AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- C) Credit Note lines  (reverse of B)
    --------------------------------------------------------
    SELECT
        cn.CreditNoteDate                     AS TransDate,
        'CN'                                  AS SourceType,
        cn.CreditNoteNo                       AS SourceNo,
        'Credit Note ' + ISNULL(cn.SiNumber,'') AS Description,
        coa.Id                                AS HeadId,
        ISNULL(TRY_CONVERT(decimal(18,2), cnl.LineNet),0)
      + ISNULL(TRY_CONVERT(decimal(18,2), cnl.Tax),0)      AS Debit,
        CAST(0 AS decimal(18,2))              AS Credit
    FROM dbo.CreditNoteLine cnl
    INNER JOIN dbo.CreditNote cn
        ON cn.Id = cnl.CreditNoteId AND cn.IsActive = 1
    INNER JOIN dbo.Item i
        ON i.Id = cnl.ItemId AND i.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = i.BudgetLineId AND coa.IsActive = 1
    WHERE cn.CreditNoteDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                        AND ISNULL(@ToDate,'9999-12-31')
    UNION ALL

    --------------------------------------------------------
    -- D) AR – Invoice  (Debit customer AR head)
    --------------------------------------------------------
    SELECT
        si.InvoiceDate                        AS TransDate,
        'ARINV'                               AS SourceType,
        si.InvoiceNo                          AS SourceNo,
        'AR From Invoice ' + si.InvoiceNo     AS Description,
        c.BudgetLineId                        AS HeadId,
        ISNULL(TRY_CONVERT(decimal(18,2), si.total),0) AS Debit,
        CAST(0 AS decimal(18,2))              AS Credit
    FROM dbo.SalesInvoice si
    LEFT JOIN dbo.DeliveryOrder d
        ON d.Id = si.DoId
    INNER JOIN dbo.SalesOrder so
        ON so.Id = COALESCE(si.SoId, d.SoId)
    INNER JOIN dbo.Customer c
        ON c.Id = so.CustomerId AND c.IsActive = 1
    WHERE si.IsActive = 1
      AND si.InvoiceDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                        AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- E) AR – Receipt  (Credit customer AR head)
    --------------------------------------------------------
    SELECT
        r.ReceiptDate                         AS TransDate,
        'ARREC'                               AS SourceType,
        r.ReceiptNo                           AS SourceNo,
        'AR Receipt'                          AS Description,
        c.BudgetLineId                        AS HeadId,
        CAST(0 AS decimal(18,2))              AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), r.AmountReceived),0) AS Credit
    FROM dbo.ArReceipt r
    INNER JOIN dbo.Customer c
        ON c.Id = r.CustomerId AND c.IsActive = 1
    WHERE r.IsActive = 1
      AND r.ReceiptDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                        AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

     --------------------------------------------------------
    -- E2) AR – Receipt : BANK (Debit bank)
    --------------------------------------------------------
    SELECT
        r.ReceiptDate AS TransDate,
        'ARRECBNK'    AS SourceType,
        r.ReceiptNo   AS SourceNo,
        'AR Receipt - ' + ISNULL(b.BankName,'') AS Description,
        b.BudgetLineId AS HeadId,           -- BANK COA
        ISNULL(TRY_CONVERT(decimal(18,2), r.AmountReceived),0) AS Debit,
        CAST(0 AS decimal(18,2)) AS Credit
    FROM dbo.ArReceipt r
    INNER JOIN dbo.Bank b
        ON b.Id = r.BankId AND b.IsActive = 1
    WHERE r.IsActive = 1
      AND r.PaymentMode = 'BANK'           -- 🔹 only BANK here
      AND r.ReceiptDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                            AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- E3) AR – Receipt : CASH (Debit Cash-in-hand)
    --------------------------------------------------------
    SELECT
        r.ReceiptDate AS TransDate,
        'ARRECCASH'   AS SourceType,
        r.ReceiptNo   AS SourceNo,
        'AR Receipt - Cash' AS Description,
        cash.CashHeadId AS HeadId,         -- 🔹 CASH COA
        ISNULL(TRY_CONVERT(decimal(18,2), r.AmountReceived),0) AS Debit,
        CAST(0 AS decimal(18,2)) AS Credit
    FROM dbo.ArReceipt r
    CROSS JOIN CashCoa cash
    WHERE r.IsActive = 1
      AND r.PaymentMode = 'CASH'           -- 🔹 only CASH here
      AND r.ReceiptDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                            AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- F) AR – Credit Note (Credit customer AR head)
    --------------------------------------------------------
    SELECT
        cn.CreditNoteDate                     AS TransDate,
        'ARCN'                                AS SourceType,
        cn.CreditNoteNo                       AS SourceNo,
        'AR Credit Note'                      AS Description,
        c.BudgetLineId                        AS HeadId,
        CAST(0 AS decimal(18,2))              AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), cn.Subtotal),0) AS Credit
    FROM dbo.CreditNote cn
    INNER JOIN dbo.Customer c
        ON c.Id = cn.CustomerId AND c.IsActive = 1
    WHERE cn.IsActive = 1
      AND cn.CreditNoteDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                        AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

  --------------------------------------------------------
-- G1) AP – Supplier Invoice LINES (PINL -> item/budgetLineId)
--     Distribute si.Amount (inclusive of tax) across lines
--------------------------------------------------------
SELECT
    si.InvoiceDate AS TransDate,
    'PINL'         AS SourceType,
    si.InvoiceNo   AS SourceNo,
    'PIN Line - ' + ISNULL(i.ItemName,'') AS Description,
    coa.Id         AS HeadId,

    Debit =
        CASE 
            -- if, for some reason, total lineTotal = 0
            WHEN ISNULL(SUM(j.LineTotal) OVER (PARTITION BY si.Id),0) = 0 THEN
                ISNULL(TRY_CONVERT(decimal(18,2), si.Amount),0)
            ELSE
                ISNULL(TRY_CONVERT(decimal(18,2), si.Amount),0)
                * ISNULL(j.LineTotal,0)
                / NULLIF(SUM(j.LineTotal) OVER (PARTITION BY si.Id),0)
        END,

    Credit = CAST(0 AS decimal(18,2))

FROM dbo.SupplierInvoicePin si
CROSS APPLY OPENJSON(si.LinesJson) WITH (
    ItemCode     nvarchar(50)  '$.item',
    LineTotal    decimal(18,2) '$.lineTotal',
    BudgetLineId int           '$.budgetLineId'
) AS j
INNER JOIN dbo.Item i
    ON i.ItemCode = j.ItemCode
   AND i.IsActive = 1
INNER JOIN dbo.ChartOfAccount coa
    ON coa.Id = COALESCE(j.BudgetLineId, i.BudgetLineId)
   AND coa.IsActive = 1
WHERE si.IsActive = 1
  AND si.Status   = 3
  AND si.InvoiceDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                         AND ISNULL(@ToDate,'9999-12-31')




    UNION ALL

   --------------------------------------------------------
-- G2) AP – Supplier Invoice (PIN -> supplier head)
--------------------------------------------------------
SELECT
    si.InvoiceDate AS TransDate,
    'PIN'          AS SourceType,
    si.InvoiceNo   AS SourceNo,
    'Supplier Invoice' AS Description,
    s.BudgetLineId AS HeadId,
    CAST(0 AS decimal(18,2)) AS Debit,
    ISNULL(TRY_CONVERT(decimal(18,2), si.Amount),0) AS Credit
FROM dbo.SupplierInvoicePin si
INNER JOIN dbo.Suppliers s
    ON s.Id = si.SupplierId AND s.IsActive = 1
WHERE si.IsActive = 1
  AND si.Status   = 3
  AND si.InvoiceDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                         AND ISNULL(@ToDate,'9999-12-31')


    UNION ALL

    --------------------------------------------------------
    -- H) AP – Supplier Payment  (Debit AP – Supplier head)
    --------------------------------------------------------
    SELECT
        sp.PaymentDate AS TransDate,
        'SPAY'         AS SourceType,
        sp.PaymentNo   AS SourceNo,
        'Supplier Payment' AS Description,
        s.BudgetLineId AS HeadId,           -- AP COA
        ISNULL(TRY_CONVERT(decimal(18,2), sp.Amount),0) AS Debit,
        CAST(0 AS decimal(18,2)) AS Credit
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Suppliers s
        ON s.Id = sp.SupplierId AND s.IsActive = 1
    WHERE sp.IsActive = 1
      AND sp.Status   = 1
      AND sp.PaymentDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                        AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

      --------------------------------------------------------
    -- H2) AP – Supplier Payment : BANK (Credit bank)
    --------------------------------------------------------
    SELECT
        sp.PaymentDate AS TransDate,
        'SPAYBNK'      AS SourceType,
        sp.PaymentNo   AS SourceNo,
        'Supplier Payment - ' + ISNULL(b.BankName,'') AS Description,
        b.BudgetLineId AS HeadId,             -- BANK COA
        CAST(0 AS decimal(18,2)) AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), sp.Amount),0) AS Credit
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Bank b
        ON b.Id = sp.BankId AND b.IsActive = 1
    WHERE sp.IsActive = 1
      AND sp.Status   = 1
      AND (sp.PaymentMethodId <> 1 OR sp.PaymentMethodId IS NULL) -- 🔹 NOT cash
      AND sp.PaymentDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                             AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- H3) AP – Supplier Payment : CASH (Credit Cash-in-hand)
    --------------------------------------------------------
    SELECT
        sp.PaymentDate AS TransDate,
        'SPAYCASH'     AS SourceType,
        sp.PaymentNo   AS SourceNo,
        'Supplier Payment - Cash' AS Description,
        cash.CashHeadId AS HeadId,            -- 🔹 CASH COA
        CAST(0 AS decimal(18,2)) AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), sp.Amount),0) AS Credit
    FROM dbo.SupplierPayment sp
    CROSS JOIN CashCoa cash
    WHERE sp.IsActive = 1
      AND sp.Status   = 1
      AND sp.PaymentMethodId = 1              -- 🔹 1 = CASH
      AND sp.PaymentDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                             AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- I) AP – Supplier Debit Note (Debit AP)
    --------------------------------------------------------
    SELECT
        dn.NoteDate                           AS TransDate,
        'SDN'                                 AS SourceType,
        dn.DebitNoteNo                        AS SourceNo,
        ISNULL(dn.Reason,'Supplier Debit Note') AS Description,
        s.BudgetLineId                        AS HeadId,
        ISNULL(ABS(TRY_CONVERT(decimal(18,2), dn.Amount)),0) AS Debit,
        CAST(0 AS decimal(18,2))              AS Credit
    FROM dbo.SupplierDebitNote dn
    INNER JOIN dbo.Suppliers s
        ON s.Id = dn.SupplierId AND s.IsActive = 1
    WHERE dn.IsActive = 1
      AND dn.Status   = 2
      AND dn.NoteDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                        AND ISNULL(@ToDate,'9999-12-31')
),

------------------------------------------------------------
-- 2) OPENING BALANCE TABLE (per COA HeadId)
------------------------------------------------------------
OpeningBase AS (
    SELECT
        ob.BudgetLineId AS HeadId,
        OpeningBalanceBase = SUM(ISNULL(ob.OpeningBalanceAmount,0))
    FROM dbo.OpeningBalance ob
    WHERE ob.IsActive = 1
    GROUP BY ob.BudgetLineId
),


------------------------------------------------------------
-- 2) MOVEMENTS PER LEAF ACCOUNT
------------------------------------------------------------
Movements AS (
    SELECT
        coa.Id          AS HeadId,
        coa.HeadCode,
        coa.HeadName,
        coa.HeadType,
        coa.ParentHead,

        OpeningMov = SUM(
            CASE 
                WHEN gl.TransDate < ISNULL(@FromDate,'1900-01-01')
                    THEN ISNULL(gl.Debit,0) - ISNULL(gl.Credit,0)
                ELSE 0
            END
        ),

        PeriodMov = SUM(
            CASE 
                WHEN gl.TransDate >= ISNULL(@FromDate,'1900-01-01')
                 AND gl.TransDate <= ISNULL(@ToDate,'9999-12-31')
                    THEN ISNULL(gl.Debit,0) - ISNULL(gl.Credit,0)
                ELSE 0
            END
        ),

        -- ✅ FIXED LINE
        OpeningBalanceBase = ISNULL(ob.OpeningBalanceBase, 0)

    FROM dbo.ChartOfAccount coa
    LEFT JOIN GlLines     gl ON gl.HeadId = coa.Id
    LEFT JOIN OpeningBase ob ON ob.HeadId = coa.Id
    WHERE coa.IsActive = 1
    GROUP BY
        coa.Id,
        coa.HeadCode,
        coa.HeadName,
        coa.HeadType,
        coa.ParentHead,
        ob.OpeningBalanceBase
),


------------------------------------------------------------
-- 3) SIGNED OPENING / CLOSING BALANCE (per account)
------------------------------------------------------------
SignedRaw AS (
    SELECT
        HeadId,
        HeadCode,
        HeadName,
        HeadType,
        ParentHead,
        OpeningBalanceBase,
        OpeningMov,
        PeriodMov,
        OpeningSigned_raw =
            OpeningBalanceBase + OpeningMov,
        ClosingSigned_raw  =
            OpeningBalanceBase + OpeningMov + PeriodMov
    FROM Movements
)
------------------------------------------------------------
-- 4) FINAL: ONE ROW PER HEAD (no prefix rollup)
------------------------------------------------------------
SELECT
    HeadId,
    HeadCode,
    HeadName,
    ParentHead,
    OpeningDebit  = CASE WHEN OpeningSigned_raw > 0 THEN OpeningSigned_raw ELSE 0 END,
    OpeningCredit = CASE WHEN OpeningSigned_raw < 0 THEN -OpeningSigned_raw ELSE 0 END,
    ClosingDebit  = CASE WHEN ClosingSigned_raw > 0 THEN ClosingSigned_raw ELSE 0 END,
    ClosingCredit = CASE WHEN ClosingSigned_raw < 0 THEN -ClosingSigned_raw ELSE 0 END
FROM SignedRaw
ORDER BY HeadCode;


";

            return await Connection.QueryAsync<TrialBalanceDTO>(
                sql,
                new
                {
                    dto.FromDate,
                    dto.ToDate,
                    dto.CompanyId
                });
        }


        // ============================================================
        //  TRIAL BALANCE DETAIL  (includes bank legs & PINL)
        // ============================================================

        public async Task<IEnumerable<TrialBalanceDetailDTO>> GetTrialBalanceDetailAsync(
    TrialBalanceDetailRequestDTO dto)
        {
            const string sql = @"
;WITH

------------------------------------------------------------
-- 0) CASH COA (Assets >> Current Assets >> Cash-in-hand >> Cash)
------------------------------------------------------------
CashCoa AS (
    SELECT TOP (1) Id AS CashHeadId
    FROM dbo.ChartOfAccount
    WHERE HeadName = 'Cash'   -- adjust name if different
      AND IsActive = 1
),

------------------------------------------------------------
-- 1) VIRTUAL GL LINES (ALL MODULES)
------------------------------------------------------------
GlLines AS (

     --------------------------------------------------------
    -- A) Manual Journal – single leg per line (all accounts)
    --------------------------------------------------------
    SELECT
        mj.JournalDate                          AS TransDate,
        'MJ'                                    AS SourceType,
        mj.JournalNo                            AS SourceNo,
        ISNULL(mjl.LineDescription, mj.Description) AS Description,
        mjl.AccountId                           AS HeadId,
        ISNULL(TRY_CONVERT(decimal(18,2), mjl.Debit), 0)  AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), mjl.Credit), 0) AS Credit
    FROM dbo.ManualJournal mj
    INNER JOIN dbo.ManualJournalLine mjl
        ON mjl.JournalId = mj.Id
       AND mjl.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = mjl.AccountId
       AND coa.IsActive = 1
    WHERE mj.IsActive = 1
      AND mj.IsPosted = 1
      AND mj.JournalDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                             AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL


    --------------------------------------------------------
    -- B) SI lines (P&L side, using line BudgetLineId/Item)
    --------------------------------------------------------
    SELECT
        si.InvoiceDate AS TransDate,
        'SI'           AS SourceType,
        si.InvoiceNo   AS SourceNo,
        ISNULL(sil.Description,'') AS Description,
        coa.Id         AS HeadId,
        CAST(0 AS decimal(18,2)) AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), sil.LineAmount),0)
      + ISNULL(TRY_CONVERT(decimal(18,2), sil.Tax),0)     AS Credit
    FROM dbo.SalesInvoiceLine sil
    INNER JOIN dbo.SalesInvoice si
        ON si.Id = sil.SiId AND si.IsActive = 1
    INNER JOIN dbo.Item i
        ON i.Id = sil.ItemId AND i.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = COALESCE(sil.BudgetLineId, i.BudgetLineId)
       AND coa.IsActive = 1
    WHERE si.InvoiceDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                             AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- C) CN lines (reverse P&L, using item BudgetLineId)
    --------------------------------------------------------
    SELECT
        cn.CreditNoteDate AS TransDate,
        'CN'              AS SourceType,
        cn.CreditNoteNo   AS SourceNo,
        'Credit Note ' + ISNULL(cn.SiNumber,'') AS Description,
        coa.Id            AS HeadId,
        ISNULL(TRY_CONVERT(decimal(18,2), cnl.LineNet),0)
      + ISNULL(TRY_CONVERT(decimal(18,2), cnl.Tax),0)     AS Debit,
        CAST(0 AS decimal(18,2)) AS Credit
    FROM dbo.CreditNoteLine cnl
    INNER JOIN dbo.CreditNote cn
        ON cn.Id = cnl.CreditNoteId AND cn.IsActive = 1
    INNER JOIN dbo.Item i
        ON i.Id = cnl.ItemId AND i.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = i.BudgetLineId AND coa.IsActive = 1
    WHERE cn.CreditNoteDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                                AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- D) AR – Invoice
    --------------------------------------------------------
    SELECT
        si.InvoiceDate AS TransDate,
        'ARINV'        AS SourceType,
        si.InvoiceNo   AS SourceNo,
        'AR From Invoice ' + si.InvoiceNo AS Description,
        c.BudgetLineId AS HeadId,
        ISNULL(TRY_CONVERT(decimal(18,2), si.total),0) AS Debit,
        CAST(0 AS decimal(18,2)) AS Credit
    FROM dbo.SalesInvoice si
    LEFT JOIN dbo.DeliveryOrder d
        ON d.Id = si.DoId
    INNER JOIN dbo.SalesOrder so
        ON so.Id = COALESCE(si.SoId, d.SoId)
    INNER JOIN dbo.Customer c
        ON c.Id = so.CustomerId
       AND c.IsActive = 1
    WHERE si.IsActive = 1
      AND si.InvoiceDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                             AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- E) AR – Receipt  (Credit AR)
    --------------------------------------------------------
    SELECT
        r.ReceiptDate AS TransDate,
        'ARREC'       AS SourceType,
        r.ReceiptNo   AS SourceNo,
        'AR Receipt'  AS Description,
        c.BudgetLineId AS HeadId,
        CAST(0 AS decimal(18,2)) AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), r.AmountReceived),0) AS Credit
    FROM dbo.ArReceipt r
    INNER JOIN dbo.Customer c
        ON c.Id = r.CustomerId AND c.IsActive = 1
    WHERE r.IsActive = 1
      AND r.ReceiptDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                            AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

     --------------------------------------------------------
    -- E2) AR – Receipt : BANK (Debit bank)
    --------------------------------------------------------
    SELECT
        r.ReceiptDate AS TransDate,
        'ARRECBNK'    AS SourceType,
        r.ReceiptNo   AS SourceNo,
        'AR Receipt - ' + ISNULL(b.BankName,'') AS Description,
        b.BudgetLineId AS HeadId,           -- BANK COA
        ISNULL(TRY_CONVERT(decimal(18,2), r.AmountReceived),0) AS Debit,
        CAST(0 AS decimal(18,2)) AS Credit
    FROM dbo.ArReceipt r
    INNER JOIN dbo.Bank b
        ON b.Id = r.BankId AND b.IsActive = 1
    WHERE r.IsActive = 1
      AND r.PaymentMode = 'BANK'           -- 🔹 only BANK here
      AND r.ReceiptDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                            AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- E3) AR – Receipt : CASH (Debit Cash-in-hand)
    --------------------------------------------------------
    SELECT
        r.ReceiptDate AS TransDate,
        'ARRECCASH'   AS SourceType,
        r.ReceiptNo   AS SourceNo,
        'AR Receipt - Cash' AS Description,
        cash.CashHeadId AS HeadId,         -- 🔹 CASH COA
        ISNULL(TRY_CONVERT(decimal(18,2), r.AmountReceived),0) AS Debit,
        CAST(0 AS decimal(18,2)) AS Credit
    FROM dbo.ArReceipt r
    CROSS JOIN CashCoa cash
    WHERE r.IsActive = 1
      AND r.PaymentMode = 'CASH'           -- 🔹 only CASH here
      AND r.ReceiptDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                            AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- F) AR – Credit Note
    --------------------------------------------------------
    SELECT
        cn.CreditNoteDate AS TransDate,
        'ARCN'            AS SourceType,
        cn.CreditNoteNo   AS SourceNo,
        'AR Credit Note'  AS Description,
        c.BudgetLineId    AS HeadId,
        CAST(0 AS decimal(18,2)) AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), cn.Subtotal),0) AS Credit
    FROM dbo.CreditNote cn
    INNER JOIN dbo.Customer c
        ON c.Id = cn.CustomerId AND c.IsActive = 1
    WHERE cn.IsActive = 1
      AND cn.CreditNoteDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                                AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

  --------------------------------------------------------
-- G1) AP – Supplier Invoice LINES (PINL -> item/budgetLineId)
--     Distribute si.Amount (inclusive of tax) across lines
--------------------------------------------------------
SELECT
    si.InvoiceDate AS TransDate,
    'PINL'         AS SourceType,
    si.InvoiceNo   AS SourceNo,
    'PIN Line - ' + ISNULL(i.ItemName,'') AS Description,
    coa.Id         AS HeadId,

    Debit =
        CASE 
            -- if, for some reason, total lineTotal = 0
            WHEN ISNULL(SUM(j.LineTotal) OVER (PARTITION BY si.Id),0) = 0 THEN
                ISNULL(TRY_CONVERT(decimal(18,2), si.Amount),0)
            ELSE
                ISNULL(TRY_CONVERT(decimal(18,2), si.Amount),0)
                * ISNULL(j.LineTotal,0)
                / NULLIF(SUM(j.LineTotal) OVER (PARTITION BY si.Id),0)
        END,

    Credit = CAST(0 AS decimal(18,2))

FROM dbo.SupplierInvoicePin si
CROSS APPLY OPENJSON(si.LinesJson) WITH (
    ItemCode     nvarchar(50)  '$.item',
    LineTotal    decimal(18,2) '$.lineTotal',
    BudgetLineId int           '$.budgetLineId'
) AS j
INNER JOIN dbo.Item i
    ON i.ItemCode = j.ItemCode
   AND i.IsActive = 1
INNER JOIN dbo.ChartOfAccount coa
    ON coa.Id = COALESCE(j.BudgetLineId, i.BudgetLineId)
   AND coa.IsActive = 1
WHERE si.IsActive = 1
  AND si.Status   = 3
  AND si.InvoiceDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                         AND ISNULL(@ToDate,'9999-12-31')




    UNION ALL

   --------------------------------------------------------
-- G2) AP – Supplier Invoice (PIN -> supplier head)
--------------------------------------------------------
SELECT
    si.InvoiceDate AS TransDate,
    'PIN'          AS SourceType,
    si.InvoiceNo   AS SourceNo,
    'Supplier Invoice' AS Description,
    s.BudgetLineId AS HeadId,
    CAST(0 AS decimal(18,2)) AS Debit,
    ISNULL(TRY_CONVERT(decimal(18,2), si.Amount),0) AS Credit
FROM dbo.SupplierInvoicePin si
INNER JOIN dbo.Suppliers s
    ON s.Id = si.SupplierId AND s.IsActive = 1
WHERE si.IsActive = 1
  AND si.Status   = 3
  AND si.InvoiceDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                         AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- H) AP – Supplier Payment  (Debit AP)
    --------------------------------------------------------
    SELECT
        sp.PaymentDate AS TransDate,
        'SPAY'         AS SourceType,
        sp.PaymentNo   AS SourceNo,
        'Supplier Payment' AS Description,
        s.BudgetLineId AS HeadId,
        ISNULL(TRY_CONVERT(decimal(18,2), sp.Amount),0) AS Debit,
        CAST(0 AS decimal(18,2)) AS Credit
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Suppliers s
        ON s.Id = sp.SupplierId AND s.IsActive = 1
    WHERE sp.IsActive = 1
      AND sp.Status   = 1
      AND sp.PaymentDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                             AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

     --------------------------------------------------------
    -- H2) AP – Supplier Payment : BANK (Credit bank)
    --------------------------------------------------------
    SELECT
        sp.PaymentDate AS TransDate,
        'SPAYBNK'      AS SourceType,
        sp.PaymentNo   AS SourceNo,
        'Supplier Payment - ' + ISNULL(b.BankName,'') AS Description,
        b.BudgetLineId AS HeadId,             -- BANK COA
        CAST(0 AS decimal(18,2)) AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), sp.Amount),0) AS Credit
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Bank b
        ON b.Id = sp.BankId AND b.IsActive = 1
    WHERE sp.IsActive = 1
      AND sp.Status   = 1
      AND (sp.PaymentMethodId <> 1 OR sp.PaymentMethodId IS NULL) -- 🔹 NOT cash
      AND sp.PaymentDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                             AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- H3) AP – Supplier Payment : CASH (Credit Cash-in-hand)
    --------------------------------------------------------
    SELECT
        sp.PaymentDate AS TransDate,
        'SPAYCASH'     AS SourceType,
        sp.PaymentNo   AS SourceNo,
        'Supplier Payment - Cash' AS Description,
        cash.CashHeadId AS HeadId,            -- 🔹 CASH COA
        CAST(0 AS decimal(18,2)) AS Debit,
        ISNULL(TRY_CONVERT(decimal(18,2), sp.Amount),0) AS Credit
    FROM dbo.SupplierPayment sp
    CROSS JOIN CashCoa cash
    WHERE sp.IsActive = 1
      AND sp.Status   = 1
      AND sp.PaymentMethodId = 1              -- 🔹 1 = CASH
      AND sp.PaymentDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                             AND ISNULL(@ToDate,'9999-12-31')

    UNION ALL

    --------------------------------------------------------
    -- I) AP – Supplier Debit Note
    --------------------------------------------------------
    SELECT
        dn.NoteDate     AS TransDate,
        'SDN'           AS SourceType,
        dn.DebitNoteNo  AS SourceNo,
        ISNULL(dn.Reason,'Supplier Debit Note') AS Description,
        s.BudgetLineId  AS HeadId,
        ISNULL(ABS(TRY_CONVERT(decimal(18,2), dn.Amount)),0) AS Debit,
        CAST(0 AS decimal(18,2)) AS Credit
    FROM dbo.SupplierDebitNote dn
    INNER JOIN dbo.Suppliers s
        ON s.Id = dn.SupplierId AND s.IsActive = 1
    WHERE dn.IsActive = 1
      AND dn.Status   = 2
      AND dn.NoteDate BETWEEN ISNULL(@FromDate,'1900-01-01')
                          AND ISNULL(@ToDate,'9999-12-31')
)
SELECT
    TransDate,
    SourceType,
    SourceNo,
    Description,
    Debit,
    Credit
FROM GlLines
WHERE HeadId = @HeadId
ORDER BY TransDate, SourceType, SourceNo;
";

            return await Connection.QueryAsync<TrialBalanceDetailDTO>(
                sql,
                new
                {
                    dto.HeadId,
                    dto.FromDate,
                    dto.ToDate,
                    dto.CompanyId
                });
        }
        public async Task<IEnumerable<ProfitLossViewInfo>> GetProfitLossDetails()
        {
            const string sql = @"


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
    -- 2 ► Manual Journal (header + lines)
    --    New structure: header has IsPosted/IsActive, lines have AccountId/Debit/Credit
    --------------------------------------------------------
    SELECT
        mjl.AccountId AS HeadId,         -- assumes AccountId = ChartOfAccount.Id
        OpeningBalance = 0,
        Debit  = ISNULL(mjl.Debit,  0),
        Credit = ISNULL(mjl.Credit, 0)
    FROM dbo.ManualJournalLine mjl
    INNER JOIN dbo.ManualJournal mj
        ON mj.Id = mjl.JournalId
    WHERE mj.IsActive   = 1
      AND mj.IsPosted   = 1
      AND mjl.IsActive  = 1

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

    -- 3b) DR Customer via DO → SO
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

    -- 3c) CR Income / Asset (item line’s BudgetLineId)
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
    -- 4a) CR Supplier
    SELECT
        sps.BudgetLineId AS HeadId,
        0,
        0,
        Credit = ISNULL(sip.Amount, 0)
    FROM dbo.SupplierInvoicePin sip
    INNER JOIN dbo.Suppliers sps ON sps.Id = sip.SupplierId
    WHERE sip.IsActive = 1

    UNION ALL

    -- 4b) DR Expense / Asset from PIN lines  (use sip.Amount)
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
      AND (
            cashCoa.HeadName = 'Cash'
         OR cashCoa.HeadCode = 10102
      )

    UNION ALL

    -- 5c) CR Customer
    SELECT
        coaCust.Id AS HeadId,
        0,
        0,
        Credit = ISNULL(ra.AllocatedAmount, 0)
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
    -- 6 ► Supplier Payment – DR Supplier, CR Bank/Cash
    --------------------------------------------------------
    -- 6a) DR Supplier
    SELECT
        sps.BudgetLineId AS HeadId,
        0,
        Debit  = ISNULL(sp.Amount, 0),
        Credit = 0
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Suppliers sps ON sps.Id = sp.SupplierId
    WHERE sp.IsActive = 1

    UNION ALL

    -- 6b) CR Bank (non-cash)
    SELECT
        bk.BudgetLineId AS HeadId,
        0,
        Debit  = 0,
        Credit = ISNULL(sp.Amount, 0)
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Bank bk ON bk.Id = sp.BankId
    WHERE sp.IsActive = 1
      AND sp.PaymentMethodId <> 1

    UNION ALL

    -- 6c) CR Cash (cash)
    SELECT
        cashCoa.Id AS HeadId,
        0,
        Debit  = 0,
        Credit = ISNULL(sp.Amount, 0)
    FROM dbo.SupplierPayment sp
    CROSS JOIN dbo.ChartOfAccount cashCoa
    WHERE sp.IsActive = 1
      AND sp.PaymentMethodId = 1
      AND cashCoa.IsActive = 1
      AND cashCoa.HeadName = 'Cash'

    UNION ALL

    --------------------------------------------------------
    -- 7 ► Supplier Debit Note – DR Supplier, CR Expense/Income
    --------------------------------------------------------
    -- 7a) DR Supplier
    SELECT
        sps.BudgetLineId AS HeadId,
        0,
        Debit = ISNULL(sdn.Amount, 0),
        0
    FROM dbo.SupplierDebitNote sdn
    INNER JOIN dbo.Suppliers sps ON sps.Id = sdn.SupplierId
    WHERE sdn.IsActive = 1

    UNION ALL

    -- 7b) CR Expense/Income (lines)
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
    -- 8a) DR Income (item’s account)
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

    -- 8b) CR Customer
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
-- D) Profit & Loss rows: use ROOT HEAD NAME (Income / Expense)
------------------------------------------------------------
PlRows AS (
    SELECT
        c.HeadName,
        c.HeadCode,
        RootName = root.HeadName,
        Balance  = ISNULL(ga.Balance, 0),

        -- EXPENSE roots → Purchase
        PurchaseVal =
            CASE
                WHEN root.HeadName IN ('Expense','Expenses','EXPENSE','EXPENSES')
                    THEN ISNULL(ga.Balance, 0)
                ELSE 0
            END,

        -- INCOME roots → Sales  (flip sign)
        SalesVal =
            CASE
                WHEN root.HeadName IN ('Income','Incomes','INCOME','INCOMES')
                    THEN -ISNULL(ga.Balance, 0)
                ELSE 0
            END
    FROM dbo.ChartOfAccount c
    INNER JOIN CoaRoot cr
        ON cr.Id = c.Id
    INNER JOIN dbo.ChartOfAccount root
        ON root.Id = cr.RootHeadId
    LEFT JOIN GlAgg ga
        ON ga.HeadId = c.Id
    WHERE c.IsActive = 1
      AND root.HeadName IN (
            'Income','Incomes','INCOME','INCOMES',
            'Expense','Expenses','EXPENSE','EXPENSES'
          )
)

------------------------------------------------------------
-- E) Final SELECT mapped to ProfitLossViewInfo DTO
------------------------------------------------------------
SELECT
    HeadName = PlRows.HeadName,
    HeadCode = PlRows.HeadCode,

    Purchase = CAST(ROUND(PurchaseVal, 0) AS int),
    Sales    = CAST(ROUND(SalesVal,    0) AS int),

    NetProfit = CAST(
                    ROUND(
                        SalesVal - PurchaseVal, 0
                    ) AS int
                )
FROM PlRows
ORDER BY HeadCode;

";

            return await Connection.QueryAsync<ProfitLossViewInfo>(sql);
        }

        public async Task<IEnumerable<BalanceSheetViewInfo>> GetBalanceSheetAsync()
        {
            const string sql = @"
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
    -- 2 ► Manual Journal (header + lines, NEW structure)
    --------------------------------------------------------
    SELECT
        mjl.AccountId AS HeadId,         -- assumes AccountId = ChartOfAccount.Id
        OpeningBalance = 0,
        Debit  = ISNULL(mjl.Debit,  0),
        Credit = ISNULL(mjl.Credit, 0)
    FROM dbo.ManualJournalLine mjl
    INNER JOIN dbo.ManualJournal mj
        ON mj.Id = mjl.JournalId
    WHERE mj.IsActive   = 1
      AND mj.IsPosted   = 1
      AND mjl.IsActive  = 1

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

    -- 3b) DR Customer via DO → SO
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

    -- 3c) CR Income / Asset (item line’s BudgetLineId)
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
    -- 4a) CR Supplier
    SELECT
        sps.BudgetLineId AS HeadId,
        0,
        0,
        Credit = ISNULL(sip.Amount, 0)
    FROM dbo.SupplierInvoicePin sip
    INNER JOIN dbo.Suppliers sps ON sps.Id = sip.SupplierId
    WHERE sip.IsActive = 1

    UNION ALL

    -- 4b) DR Expense / Asset from PIN lines
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
    -- 5a) DR Bank
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

    -- 5b) DR Cash
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
      AND (
            cashCoa.HeadName = 'Cash'
         OR cashCoa.HeadCode = 10102
      )

    UNION ALL

    -- 5c) CR Customer
    SELECT
        coaCust.Id AS HeadId,
        0,
        0,
        Credit = ISNULL(ra.AllocatedAmount, 0)
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
    -- 6 ► Supplier Payment – DR Supplier, CR Bank/Cash
    --------------------------------------------------------
    -- 6a) DR Supplier
    SELECT
        sps.BudgetLineId AS HeadId,
        0,
        Debit  = ISNULL(sp.Amount, 0),
        Credit = 0
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Suppliers sps ON sps.Id = sp.SupplierId
    WHERE sp.IsActive = 1

    UNION ALL

    -- 6b) CR Bank (non-cash)
    SELECT
        bk.BudgetLineId AS HeadId,
        0,
        Debit  = 0,
        Credit = ISNULL(sp.Amount, 0)
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Bank bk ON bk.Id = sp.BankId
    WHERE sp.IsActive = 1
      AND sp.PaymentMethodId <> 1

    UNION ALL

    -- 6c) CR Cash (cash)
    SELECT
        cashCoa.Id AS HeadId,
        0,
        Debit  = 0,
        Credit = ISNULL(sp.Amount, 0)
    FROM dbo.SupplierPayment sp
    CROSS JOIN dbo.ChartOfAccount cashCoa
    WHERE sp.IsActive = 1
      AND sp.PaymentMethodId = 1
      AND cashCoa.IsActive = 1
      AND cashCoa.HeadName = 'Cash'

    UNION ALL

    --------------------------------------------------------
    -- 7 ► Supplier Debit Note – DR Supplier, CR Expense/Income
    --------------------------------------------------------
    -- 7a) DR Supplier
    SELECT
        sps.BudgetLineId AS HeadId,
        0,
        Debit = ISNULL(sdn.Amount, 0),
        0
    FROM dbo.SupplierDebitNote sdn
    INNER JOIN dbo.Suppliers sps ON sps.Id = sdn.SupplierId
    WHERE sdn.IsActive = 1

    UNION ALL

    -- 7b) CR Expense/Income (lines)
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
    -- 8a) DR Income (item’s account)
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

    -- 8b) CR Customer
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
-- D) Balance Sheet rows (Assets / Liabilities, NO Income/Expenses)
------------------------------------------------------------
BsRows AS (
    SELECT
        Side =
            CASE 
                WHEN root.HeadName = 'Assets' THEN 'Assets'
                WHEN root.HeadName IN ('Liabilities','Equity')
                    THEN 'Liabilities'
                ELSE root.HeadName
            END,

        GroupHeadId   = cr.RootHeadId,
        GroupHeadName = root.HeadName,

        HeadId     = c.Id,
        HeadCode   = CAST(c.HeadCode AS varchar(50)),
        HeadName   = c.HeadName,
        ParentHead = c.ParentHead,
        RootHeadType = cr.RootHeadType,

        OpeningBalance = ISNULL(ga.OpeningBalance, 0),

        -- Normalised balance:
        --   Assets      → keep sign
        --   Liab/Equity → flip sign so credits show +ve
        Balance =
            CASE
                WHEN root.HeadName = 'Assets'
                    THEN ISNULL(ga.Balance, 0)
                WHEN root.HeadName IN ('Liabilities','Equity')
                    THEN -ISNULL(ga.Balance, 0)
                ELSE 0
            END
    FROM dbo.ChartOfAccount c
    INNER JOIN CoaRoot cr
        ON cr.Id = c.Id
    INNER JOIN dbo.ChartOfAccount root
        ON root.Id = cr.RootHeadId
    LEFT JOIN GlAgg ga
        ON ga.HeadId = c.Id
    WHERE c.IsActive = 1
      -- *** only these 3 roots → Expenses & Income excluded ***
      AND root.HeadName IN ('Assets','Liabilities','Equity')
)

------------------------------------------------------------
-- E) Final SELECT
------------------------------------------------------------
SELECT
    Side,
    GroupHeadId,
    GroupHeadName,
    HeadId,
    HeadCode,
    HeadName,
    ParentHead,
    RootHeadType,
    OpeningBalance,
    Balance
FROM BsRows
ORDER BY
    CASE Side WHEN 'Liabilities' THEN 0 WHEN 'Assets' THEN 1 ELSE 2 END,
    GroupHeadId,
    HeadCode;
";

            return await Connection.QueryAsync<BalanceSheetViewInfo>(sql);
        }

        public async Task<IEnumerable<DaybookDTO>> GetDaybookAsync(ReportBaseDTO dto)
        {
            const string sql = @"
;WITH DaybookRaw AS
(
    ------------------------------------------------------------
    -- 1) SALES INVOICE  (Customer AR Debit)
    ------------------------------------------------------------
    SELECT
        si.InvoiceDate        AS TransDate,
        si.InvoiceNo          AS VoucherNo,
        'SI'                  AS VoucherType,
        'Sales Invoice'       AS VoucherName,
        COALESCE(coaSi.HeadName, 'No COA (Item)') AS AccountHeadName,
        si.Remarks            AS Description,
        CAST(ISNULL(si.Total, 0.00) AS DECIMAL(18,2)) AS Debit,
        CAST(0.00 AS DECIMAL(18,2))                   AS Credit
    FROM dbo.SalesInvoice si
    OUTER APPLY
    (
        SELECT TOP (1) coa.HeadName
        FROM dbo.SalesInvoiceLine sil
        INNER JOIN dbo.Item itm
            ON itm.Id = sil.ItemId
        INNER JOIN dbo.ChartOfAccount coa
            ON coa.Id = itm.BudgetLineId
           AND coa.IsActive = 1
        WHERE sil.SiId = si.Id
        ORDER BY coa.HeadName
    ) coaSi
    WHERE si.IsActive = 1
      AND si.InvoiceDate BETWEEN @FromDate AND @ToDate

    UNION ALL

    ------------------------------------------------------------
    -- 1B) CREDIT NOTE (reverse of invoice)
    ------------------------------------------------------------
    SELECT
        cn.CreditNoteDate     AS TransDate,
        cn.CreditNoteNo       AS VoucherNo,
        'CN'                  AS VoucherType,
        'Credit Note'         AS VoucherName,
        COALESCE(coaCn.HeadName, 'No COA (Item)') AS AccountHeadName,
        CONCAT(
            'Credit Note for ',
            ISNULL(cn.CustomerName, ''),
            CASE 
                WHEN cn.SiNumber IS NOT NULL THEN ' (SI: ' + cn.SiNumber + ')' 
                ELSE '' 
            END
        )                    AS Description,
        CAST(0.00 AS DECIMAL(18,2))                    AS Debit,
        CAST(ISNULL(cn.Subtotal, 0.00) AS DECIMAL(18,2)) AS Credit
    FROM dbo.CreditNote cn
    OUTER APPLY
    (
        SELECT TOP (1) coa.HeadName
        FROM dbo.CreditNoteLine cnl
        INNER JOIN dbo.Item itm
            ON itm.Id = cnl.ItemId
        INNER JOIN dbo.ChartOfAccount coa
            ON coa.Id = itm.BudgetLineId
           AND coa.IsActive = 1
        WHERE cnl.CreditNoteId = cn.Id
          AND cnl.IsActive = 1
        ORDER BY coa.HeadName
    ) coaCn
    WHERE cn.IsActive = 1
      AND cn.CreditNoteDate BETWEEN @FromDate AND @ToDate

    UNION ALL

    ------------------------------------------------------------
    -- 2) AR RECEIPT (Customer Receipt – money coming in)
    ------------------------------------------------------------
    SELECT
        r.ReceiptDate         AS TransDate,
        r.ReceiptNo           AS VoucherNo,
        'AR-RCPT'             AS VoucherType,
        'Customer Receipt'    AS VoucherName,
        coa.HeadName          AS AccountHeadName,
        r.Remarks             AS Description,
        CAST(0.00 AS DECIMAL(18,2))                        AS Debit,
        CAST(ISNULL(r.AmountReceived, 0.00) AS DECIMAL(18,2)) AS Credit
    FROM dbo.ArReceipt r
    INNER JOIN dbo.Customer c
        ON c.Id = r.CustomerId
       AND c.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = c.BudgetLineId
       AND coa.IsActive = 1
    WHERE r.IsActive = 1
      AND r.ReceiptDate BETWEEN @FromDate AND @ToDate

    UNION ALL

    ------------------------------------------------------------
    -- 3) SUPPLIER INVOICE PIN  (AP – money we owe)
    ------------------------------------------------------------
    SELECT
        pin.InvoiceDate       AS TransDate,
        pin.InvoiceNo         AS VoucherNo,
        'PIN'                 AS VoucherType,
        'Supplier Invoice'    AS VoucherName,
        coa.HeadName          AS AccountHeadName,
        NULL                  AS Description,
        CAST(0.00 AS DECIMAL(18,2)) AS Debit,
        CAST(ISNULL(pin.Amount, 0.00) + ISNULL(pin.Tax, 0.00) AS DECIMAL(18,2)) AS Credit
    FROM dbo.SupplierInvoicePin pin
    INNER JOIN dbo.Suppliers s
        ON s.Id = pin.SupplierId
       AND s.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = s.BudgetLineId
       AND coa.IsActive = 1
    WHERE pin.IsActive = 1
      AND pin.InvoiceDate BETWEEN @FromDate AND @ToDate

    UNION ALL

    ------------------------------------------------------------
    -- 4) SUPPLIER PAYMENT (AP Payment – money going out)
    ------------------------------------------------------------
    SELECT
        sp.PaymentDate        AS TransDate,
        sp.PaymentNo          AS VoucherNo,
        'SPAY'                AS VoucherType,
        'Supplier Payment'    AS VoucherName,
        coa.HeadName          AS AccountHeadName,
        sp.Notes              AS Description,
        CAST(ISNULL(sp.Amount, 0.00) AS DECIMAL(18,2)) AS Debit,
        CAST(0.00 AS DECIMAL(18,2))                     AS Credit
    FROM dbo.SupplierPayment sp
    INNER JOIN dbo.Suppliers s
        ON s.Id = sp.SupplierId
       AND s.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = s.BudgetLineId
       AND coa.IsActive = 1
    WHERE sp.IsActive = 1
      AND sp.PaymentDate BETWEEN @FromDate AND @ToDate

    UNION ALL

    ------------------------------------------------------------
    -- 5) SUPPLIER DEBIT NOTE  (usually reduces what we owe)
    ------------------------------------------------------------
    SELECT
        dn.NoteDate           AS TransDate,
        dn.DebitNoteNo        AS VoucherNo,
        'SDN'                 AS VoucherType,
        'Supplier Debit Note' AS VoucherName,
        coa.HeadName          AS AccountHeadName,
        dn.Reason             AS Description,
        CAST(ISNULL(dn.Amount, 0.00) AS DECIMAL(18,2)) AS Debit,
        CAST(0.00 AS DECIMAL(18,2))                    AS Credit
    FROM dbo.SupplierDebitNote dn
    INNER JOIN dbo.Suppliers s
        ON s.Id = dn.SupplierId
       AND s.IsActive = 1
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = s.BudgetLineId
       AND coa.IsActive = 1
    WHERE dn.IsActive = 1
      AND dn.NoteDate BETWEEN @FromDate AND @ToDate

    UNION ALL

    ------------------------------------------------------------
    -- 6) MANUAL JOURNAL  (header + lines, new structure)
    ------------------------------------------------------------
    SELECT
        mj.JournalDate        AS TransDate,
        mj.JournalNo          AS VoucherNo,
        'MJ'                  AS VoucherType,
        'Manual Journal'      AS VoucherName,
        coa.HeadName          AS AccountHeadName,
        mj.Description        AS Description,
        CAST(SUM(ISNULL(mjl.Debit,  0.00)) AS DECIMAL(18,2)) AS Debit,
        CAST(SUM(ISNULL(mjl.Credit, 0.00)) AS DECIMAL(18,2)) AS Credit
    FROM dbo.ManualJournal mj
    INNER JOIN dbo.ManualJournalLine mjl
        ON mjl.JournalId = mj.Id
    INNER JOIN dbo.ChartOfAccount coa
        ON coa.Id = mjl.AccountId
       AND coa.IsActive = 1
    WHERE mj.IsActive   = 1
      AND mj.IsPosted   = 1           -- only posted journals
      AND mjl.IsActive  = 1
      AND mj.JournalDate BETWEEN @FromDate AND @ToDate
    GROUP BY
        mj.JournalDate,
        mj.JournalNo,
        mj.Description,
        coa.HeadName
)

SELECT
    TransDate,
    VoucherNo,
    VoucherType,
    VoucherName,
    AccountHeadName,
    Description,
    Debit,
    Credit,
    RunningBalance =
        SUM(Debit - Credit) OVER (
            ORDER BY TransDate, VoucherType, VoucherNo
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
FROM DaybookRaw
ORDER BY TransDate, VoucherType, VoucherNo;
";

            return await Connection.QueryAsync<DaybookDTO>(
                sql,
                new
                {
                    dto.FromDate,
                    dto.ToDate,
                });
        }


        public async Task SaveOpeningBalanceAsync(OpeningBalanceEditDto dto, string userName)
        {
            // signed amount: +ve = net debit, -ve = net credit
            var signedAmount = dto.OpeningDebit - dto.OpeningCredit;

            const string sql = @"
MERGE dbo.OpeningBalance AS target
USING (SELECT @HeadId AS BudgetLineId) AS src
   ON target.BudgetLineId = src.BudgetLineId
WHEN MATCHED THEN
    UPDATE SET
        OpeningBalanceAmount = @OpeningBalanceAmount,
        UpdatedBy            = @UserName,
        UpdatedDate          = SYSDATETIME(),
        IsActive             = 1
WHEN NOT MATCHED THEN
    INSERT (BudgetLineId, OpeningBalanceAmount, CreatedBy, CreatedDate, IsActive)
    VALUES (@HeadId, @OpeningBalanceAmount, @UserName, SYSDATETIME(), 1);
";

            using var conn = Connection; // from DynamicRepository base

            await conn.ExecuteAsync(sql, new
            {
                HeadId = dto.HeadId,
                OpeningBalanceAmount = signedAmount,
                UserName = userName
            });
        }
    }
}


