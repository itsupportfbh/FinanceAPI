namespace FinanceApi.ModelDTO
{
    public class ItemMasterAuditDTO
    {
        public long AuditId { get; set; }
        public int ItemId { get; set; }
        public string Action { get; set; } = "";
        public DateTime OccurredAtUtc { get; set; }
        public long? UserId { get; set; }
        public string? OldValuesJson { get; set; }
        public string? NewValuesJson { get; set; }
        public string? Remarks { get; set; }
    }
}
