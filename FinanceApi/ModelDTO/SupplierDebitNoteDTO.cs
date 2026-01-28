namespace FinanceApi.ModelDTO
{
    public class SupplierDebitNoteDTO
    {
        public int Id { get; set; }
        public string? DebitNoteNo { get; set; }
        public int SupplierId { get; set; }
        public int? PinId { get; set; }
        public int? GrnId { get; set; }
        public string? ReferenceNo { get; set; }
        public string? Reason { get; set; }
        public DateTime NoteDate { get; set; }
        public decimal Amount { get; set; }
        public string? LinesJson { get; set; }
        public int Status { get; set; } = 0;
        public bool IsActive { get; set; } = true;
        public string? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public string Name {  get; set; }   
    }
}
