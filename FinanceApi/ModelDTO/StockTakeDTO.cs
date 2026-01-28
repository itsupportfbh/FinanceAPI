using System.ComponentModel.DataAnnotations.Schema;
using FinanceApi.Models;

namespace FinanceApi.ModelDTO
{
    public class StockTakeDTO : BaseEntity
    {
        public int Id { get; set; }
        //public int TakeTypeId { get; set; }
        public int WarehouseTypeId { get; set; }

        public string? WarehouseName { get; set; }

        public int? SupplierId { get; set; }

        public string? SupplierName { get; set; }

        public int? StrategyId { get; set; }

        public string? StrategyName { get; set; }

        public List<StockTakeLines> LineItems { get; set; } = new();
        public bool Freeze { get; set; }
        public int Status { get; set; }
    }
}
