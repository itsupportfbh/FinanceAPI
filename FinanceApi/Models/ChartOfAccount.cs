namespace FinanceApi.Models
{
    public class ChartOfAccount : BaseEntity
    {
        public int Id { get; set; }
        public int? HeadCode { get; set; }
        public int? HeadLevel { get; set; }
        public string? HeadName { get; set; }
        public string? HeadType { get; set; }
        public string? HeadCodeName { get; set; }
        public Boolean? IsGl { get; set; }
        public Boolean? IsTransaction { get; set; }
        public int? ParentHead { get; set; }
        public string? PHeadName { get; set; }
        public decimal? Balance { get; set; }
        public decimal? OpeningBalance { get; set; }

    }
}
