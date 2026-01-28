namespace FinanceApi.ModelDTO
{
    public class GrnAllocResult
    {
        public int UpdatedLines { get; set; }
        public int InsertedAlloc { get; set; } // if your SP returns it
    }
}
