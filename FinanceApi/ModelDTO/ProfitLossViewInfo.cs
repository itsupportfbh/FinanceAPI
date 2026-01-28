namespace FinanceApi.ModelDTO
{
    public class ProfitLossViewInfo
    {
        public string HeadName { get; set; }
        public int HeadCode { get; set; }
        public int Purchase {  get; set; }
        public int Sales { get; set; }

        public int NetProfit { get; set; }
    }
}
