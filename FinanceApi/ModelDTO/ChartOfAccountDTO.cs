namespace FinanceApi.ModelDTO
{
    public class ChartOfAccountDTO
    {
        public int Id { get; set; }
        public int? HeadCode { get; set; }
        public int? HeadLevel { get; set; }
        public string? HeadName { get; set; }
        public string? HeadType { get; set; }
        public string? HeadCodeName { get; set; }
        public bool? IsGl { get; set; }
        public bool? IsTransaction { get; set; }
        public int? ParentHead { get; set; }
        public string? PHeadName { get; set; }
        public decimal? Balance { get; set; }
        public decimal? OpeningBalance { get; set; }
        public string? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; }
    }


}
