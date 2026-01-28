using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    public class StockTake : BaseEntity
    {
        public int Id { get; set; }
        public int WarehouseTypeId { get; set; }  
        public int SupplierId { get; set; }
        //public int TakeTypeId { get; set; }
        public int? StrategyId { get; set; }      

        public List<StockTakeLines> LineItems { get; set; } = new();
        public bool Freeze { get; set; }
        //public int Status { get; set; }

        public StockTakeStatus Status { get; set; } = StockTakeStatus.Draft;

    }

    public enum StockTakeStatus : byte
    {
        Draft = 1,
        Approved = 2,
        Posted = 3
    }

    public enum StockTakeLineStatus { Draft = 1, Approved = 2, Posted = 3 }
}
