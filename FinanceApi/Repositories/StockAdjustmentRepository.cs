using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class StockAdjustmentRepository : DynamicRepository,IStockAdjustmentRepository
    {
        public StockAdjustmentRepository(IDbConnectionFactory connectionFactory)
: base(connectionFactory)
        {
        }


        public async Task<IEnumerable<BinDTO>> GetBinDetailsbywarehouseID(int id)
        {
            const string query = @"
    SELECT DISTINCT b.Id, b.BinName
    FROM Warehouse AS w
    CROSS APPLY STRING_SPLIT(w.BinID, ',') AS s
    CROSS APPLY (SELECT TRY_CAST(LTRIM(RTRIM(s.value)) AS INT) AS BinId) AS v
    JOIN Bin AS b
      ON b.Id = v.BinId
    WHERE w.Id = @Id
    ORDER BY b.BinName;";

            return await Connection.QueryAsync<BinDTO>(query, new { Id = id });
        }



        public async Task<IEnumerable<StockAdjustmentDTO>> GetItemDetailswithwarehouseandBinID(int warehouseId,int binId)
        {
            const string query = @"
   select im.Name,im.Sku,iws.Available,bin.BinName from itemMaster as im
inner join ItemWarehouseStock as iws on iws.ItemId = im.Id
inner join Bin on bin.ID = iws.BinId
where iws.WarehouseId =@warehouseId and iws.BinId = @BinId;";

            return await Connection.QueryAsync<StockAdjustmentDTO>(query, new { warehouseId = warehouseId, BinId = binId });
        }
    }
}
