using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class SalesReportRepository : DynamicRepository, ISalesReportRepository
    {
        public SalesReportRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory) { }


        public async Task<IEnumerable<SalesReportDTO>> GetSalesByItemAsync()
        {
            const string sql = @"
SELECT
    sol.ItemName,
    sol.Uom,
    sol.Quantity,
    u.Username AS SalesPerson,
	sol.CreatedDate,
    sol.Discount,
    sol.Tax,
    sol.UnitPrice,
    im.Sku,
    im.Category,
    l.Name AS Location,
    so.Subtotal   AS GrossSales,
    so.GrandTotal AS NetSales,
    ip.Price      AS PurchaseCost,
    so.GstPct,

  -- 🧮 Total Sales Value (UnitPrice × Quantity)
    (sol.UnitPrice * sol.Quantity) AS TotalSalesValue,

    -- 🧮 Total Purchase Cost (Purchase Price × Quantity)
    (ip.Price * sol.Quantity) AS TotalPurchaseCost,

    CASE 
        WHEN UPPER(sol.Tax) = 'EXCLUSIVE' THEN 
            (so.Subtotal * so.GstPct / 100)
        WHEN UPPER(sol.Tax) = 'INCLUSIVE' THEN 
            (so.GrandTotal - (so.GrandTotal / (1 + so.GstPct / 100.0)))
        ELSE 0
    END AS TaxAmount,

 
    ((sol.UnitPrice - ISNULL(ip.Price, 0)) * sol.Quantity) AS MarginAmount,


    CASE 
        WHEN ISNULL(ip.Price, 0) > 0 
            THEN ((sol.UnitPrice - ip.Price) / ip.Price) * 100
        ELSE 0 
    END AS MarginPct

FROM SalesOrderLines AS sol
INNER JOIN ItemMaster AS im 
    ON im.Id = sol.ItemId
INNER JOIN SalesOrder AS so 
    ON so.Id = sol.SalesOrderId
INNER JOIN Customer AS cr 
    ON cr.Id = so.CustomerId
INNER JOIN Location AS l 
    ON l.Id = cr.LocationId
INNER JOIN [User] AS u
    ON u.Id = sol.CreatedBy	

LEFT JOIN (
    SELECT SoId, MIN(Id) AS Id
    FROM DeliveryOrder
    GROUP BY SoId
) AS do ON do.SoId = so.Id


LEFT JOIN (
    SELECT DoId, MIN(Id) AS Id
    FROM DeliveryOrderLine
    GROUP BY DoId
) AS dol ON dol.DoId = do.Id

LEFT JOIN (
    SELECT 
        COALESCE(SoId, DoId) AS SoDoId,
        MIN(Id) AS Id
    FROM SalesInvoice
    GROUP BY COALESCE(SoId, DoId)
) AS si ON si.SoDoId = so.Id OR si.SoDoId = do.Id


LEFT JOIN (
    SELECT SiId, MIN(Id) AS Id
    FROM SalesInvoiceLine
    GROUP BY SiId
) AS sil ON sil.SiId = si.Id

OUTER APPLY (
    SELECT TOP 1 ip.Price
    FROM ItemPrice ip
    WHERE ip.ItemId     = sol.ItemId
      AND ip.WarehouseId = sol.WarehouseId   
    ORDER BY ip.CreatedDate DESC, ip.Id DESC
) AS ip;
";
            return await Connection.QueryAsync<SalesReportDTO>(sql);
        }



        public async Task<IEnumerable<SalesMarginReportViewInfo>> GetSalesMarginAsync()
        {
            const string sql = @"
SELECT
cr.CustomerName,
im.Category,
sol.ItemName,
so.GrandTotal AS NetSales,
    ip.Price      AS PurchaseCost,
	l.Name as Location,
    so.GstPct,
	 u.Username AS SalesPerson,

    CASE 
        WHEN UPPER(sol.Tax) = 'EXCLUSIVE' THEN 
            (so.Subtotal * so.GstPct / 100)
        WHEN UPPER(sol.Tax) = 'INCLUSIVE' THEN 
            (so.GrandTotal - (so.GrandTotal / (1 + so.GstPct / 100.0)))
        ELSE 0
    END AS TaxAmount,

    ((sol.UnitPrice - ISNULL(ip.Price, 0)) * sol.Quantity) AS MarginAmount,

    CASE 
        WHEN ISNULL(ip.Price, 0) > 0 
            THEN ((sol.UnitPrice - ip.Price) / ip.Price) * 100
        ELSE 0 
    END AS MarginPct,

    -- 🆕 Added columns
    si.InvoiceNo     AS SalesInvoiceNo,
    si.InvoiceDate   AS SalesInvoiceDate

FROM SalesOrderLines AS sol
INNER JOIN ItemMaster AS im 
    ON im.Id = sol.ItemId
INNER JOIN SalesOrder AS so 
    ON so.Id = sol.SalesOrderId
INNER JOIN Customer AS cr 
    ON cr.Id = so.CustomerId
INNER JOIN Location AS l 
    ON l.Id = cr.LocationId
INNER JOIN [User] AS u
    ON u.Id = sol.CreatedBy	

LEFT JOIN (
    SELECT SoId, MIN(Id) AS Id
    FROM DeliveryOrder
    GROUP BY SoId
) AS do ON do.SoId = so.Id

LEFT JOIN (
    SELECT DoId, MIN(Id) AS Id
    FROM DeliveryOrderLine
    GROUP BY DoId
) AS dol ON dol.DoId = do.Id

LEFT JOIN (
    SELECT 
        COALESCE(SoId, DoId) AS SoDoId,
        MIN(Id) AS Id
    FROM SalesInvoice
    GROUP BY COALESCE(SoId, DoId)
) AS siMap ON siMap.SoDoId = so.Id OR siMap.SoDoId = do.Id

LEFT JOIN SalesInvoice AS si
    ON si.Id = siMap.Id   -- ✅ join to get actual invoice columns

LEFT JOIN (
    SELECT SiId, MIN(Id) AS Id
    FROM SalesInvoiceLine
    GROUP BY SiId
) AS sil ON sil.SiId = si.Id

OUTER APPLY (
    SELECT TOP 1 ip.Price
    FROM ItemPrice ip
    WHERE ip.ItemId      = sol.ItemId
      AND ip.WarehouseId = sol.WarehouseId   
    ORDER BY ip.CreatedDate DESC, ip.Id DESC
) AS ip;
";
            return await Connection.QueryAsync<SalesMarginReportViewInfo>(sql);
        }
    }
}
