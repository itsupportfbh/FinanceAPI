// Repositories/PurchaseAlertRepository.cs
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Repositories;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

public class PurchaseAlertRepository : DynamicRepository, IPurchaseAlertRepository
{
    public PurchaseAlertRepository(IDbConnectionFactory f) : base(f) { }

    public async Task<IEnumerable<PurchaseAlertDTO>> GetUnreadAsync()
    {
        const string sql = @"
SELECT a.Id, a.ItemId, a.ItemName, a.RequiredQty,
       a.WarehouseId, w.Name AS WarehouseName,
       a.SupplierId,  s.Name AS SupplierName,
       a.Source, a.SourceId, a.SourceNo,
       a.Message, a.IsRead, a.CreatedDate
FROM dbo.PurchaseAlert a
LEFT JOIN dbo.Warehouse w ON w.Id = a.WarehouseId
LEFT JOIN dbo.Suppliers s ON s.Id  = a.SupplierId
WHERE a.IsRead = 0
ORDER BY a.CreatedDate DESC;";
        return await Connection.QueryAsync<PurchaseAlertDTO>(sql);
    }



    public async Task MarkReadAsync(int id)
    {
        const string sql = @"UPDATE dbo.PurchaseAlert SET IsRead=1 WHERE Id=@Id;";
        await Connection.ExecuteAsync(sql, new { Id = id });
    }

    public async Task MarkAllReadAsync()
    {
        const string sql = @"UPDATE dbo.PurchaseAlert SET IsRead=1 WHERE IsRead=0;";
        await Connection.ExecuteAsync(sql);
    }
}
