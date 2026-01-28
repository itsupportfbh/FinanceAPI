namespace FinanceApi.ModelDTO
{
    public class ApplyGrnRequest
    {
        public string? GrnNo { get; set; }
        public DateTime? ReceptionDate { get; set; }
        public string? UpdatedBy { get; set; }

        public List<ApplyGrnLine> Lines { get; set; } = new();
    }
}
