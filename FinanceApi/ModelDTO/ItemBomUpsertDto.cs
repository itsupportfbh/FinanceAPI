namespace FinanceApi.ModelDTO
{
    public class ItemBomUpsertDto
    {
        public decimal ExistingCost { get; set; }
        public decimal UnitCost { get; set; }
        public long SupplierId { get; set; }

    }
}
