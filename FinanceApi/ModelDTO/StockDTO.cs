using Microsoft.AspNetCore.Http.Connections;

namespace FinanceApi.ModelDTO
{
    public class StockDTO
    {
        public int ID { get; set; }
        public int ItemID { get; set; }

        public int FromWarehouseID {  get; set; }
        public int ToWarehouseID { get; set; }
        public int Available { get; set;}
        public int OnHand { get; set; }
        public int Reserved { get; set; }
        public int Min { get; set; }
        public int Expiry { get; set; }
        public int isApproved { get; set; }

        public long CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }

        public long UpdatedBy { get; set; }
        public DateTime UpdatedDate { get; set; }

        public int ToBinId { get; set; }
        public int? MrId { get; set; }

        public int Status { get; set; }

    }
}
