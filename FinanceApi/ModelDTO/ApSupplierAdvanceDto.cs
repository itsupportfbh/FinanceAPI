namespace FinanceApi.ModelDTO
{
    public class ApSupplierAdvanceDto
    {
        public int Id { get; set; }
        public string AdvanceNo { get; set; }
        public int SupplierId { get; set; }
        public string SupplierName { get; set; }
        public DateTime AdvanceDate { get; set; }

        public decimal OriginalAmount { get; set; }
        public decimal UtilisedAmount { get; set; }
        public decimal BalanceAmount { get; set; }
    }
    public class ApSupplierAdvanceCreateRequest
    {
        public int SupplierId { get; set; }
        public DateTime AdvanceDate { get; set; }
        public decimal Amount { get; set; }

        public string ReferenceNo { get; set; }
        public string Notes { get; set; }

        // Payment method (Cash, Bank, Cheque, Other)
        public int MethodId { get; set; }

        // 🔹 Bank master Id (from Bank table / API dropdown)
        public int? BankId { get; set; }

        // 🔹 Bank GL HeadId (for AccountBalance / COA)
        public int? BankHeadId { get; set; }

        // 🔹 Optional GRN reference
        public string GrnNo { get; set; }
    }

    public class SupplierAdvanceListRowDto
    {
        public int Id { get; set; }
        public int SupplierId { get; set; }
        public string SupplierName { get; set; }

        public string AdvanceNo { get; set; }
        public DateTime AdvanceDate { get; set; }

        public decimal OriginalAmount { get; set; }
        public decimal UtilisedAmount { get; set; }
        public decimal BalanceAmount { get; set; }
    }
    public class ArAdvanceListDto
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public string CustomerName { get; set; }
        public string AdvanceNo { get; set; }
        public DateTime AdvanceDate { get; set; }

        public int? SalesOrderId { get; set; }
        public string SalesOrderNo { get; set; }

        public decimal Amount { get; set; }
        public decimal BalanceAmount { get; set; }

        public string PaymentMode { get; set; }

        public int? BankAccountId { get; set; }
        public string BankName { get; set; }

        public string Remarks { get; set; }
    }
}
