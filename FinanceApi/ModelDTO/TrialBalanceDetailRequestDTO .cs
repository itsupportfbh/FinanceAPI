using FinanceApi.ModelDTO;   // for ReportBaseDTO

namespace FinanceApi.ModelDTO.TB
{
    public class TrialBalanceDetailRequestDTO : ReportBaseDTO
    {
        public int HeadId { get; set; }    // clicked account id from TB row
    }
}
