namespace FinanceApi.ModelDTO
{
    public class GstPeriodOptionDto
    {
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
    }

    public class GstSystemSummaryDto
    {
        public int PeriodId { get; set; }
        public string PeriodLabel { get; set; } = string.Empty;
        public decimal CollectedOnSales { get; set; }
        public decimal PaidOnPurchases { get; set; }
        public decimal AmountDue { get; set; }
    }

    public class GstReturnDto
    {
        public int Id { get; set; }
        public int PeriodId { get; set; }
        public decimal Box6OutputTax { get; set; }
        public decimal Box7InputTax { get; set; }
        public decimal Box8NetPayable { get; set; }
        public string Status { get; set; } = "OPEN"; // OPEN / LOCKED
        public GstSystemSummaryDto SystemSummary { get; set; } = new GstSystemSummaryDto();
    }

    public class GstApplyLockRequest
    {
        public int Id { get; set; }
        public int PeriodId { get; set; }
        public decimal Box6OutputTax { get; set; }
        public decimal Box7InputTax { get; set; }
        public decimal Box8NetPayable { get; set; }
    }

    public class GstF5SummaryDto
    {
        public decimal Box6OutputTax { get; set; }
        public decimal Box7InputTax { get; set; }
        public decimal Box8NetPayable { get; set; }
    }
    public class GstFinancialYearOptionDto
    {
        public int FyStartYear { get; set; }      // e.g. 2025
        public string FyLabel { get; set; } = ""; // e.g. "2025-26"
    }
    public class GstAdjustmentDto
    {
        public int Id { get; set; }
        public int PeriodId { get; set; }
        public byte LineType { get; set; }   // 1..4
        public decimal Amount { get; set; }
        public string Description { get; set; } = "";
    }
    public class GstDocRowDto
    {
        public string DocType { get; set; } = string.Empty;  // "SI" or "PIN"
        public int DocId { get; set; }
        public string DocNo { get; set; } = string.Empty;
        public DateTime DocDate { get; set; }
        public string PartyName { get; set; } = string.Empty;
        public decimal TaxAmount { get; set; }
        public decimal NetAmount { get; set; }
    }
    public class GstDetailRowDto
    {
        public string DocType { get; set; } = string.Empty;  // 'SI' or 'PIN'
        public int DocId { get; set; }
        public string DocNo { get; set; } = string.Empty;
        public DateTime DocDate { get; set; }
        public string PartyName { get; set; } = string.Empty;
        public string Source { get; set; } = string.Empty;   // 'OUTPUT' or 'INPUT'
        public decimal TaxableAmount { get; set; }
        public decimal TaxAmount { get; set; }
        public decimal NetAmount { get; set; }
    }
}
