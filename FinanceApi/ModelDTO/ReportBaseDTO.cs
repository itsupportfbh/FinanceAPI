// ModelDTO/ReportBaseDTO.cs
namespace FinanceApi.ModelDTO
{
    public class ReportBaseDTO
    {
        public DateTime? FromDate { get; set; }
        public DateTime? ToDate { get; set; }
        public int? CompanyId { get; set; }   // optional, if you have multi-company
    }
}
