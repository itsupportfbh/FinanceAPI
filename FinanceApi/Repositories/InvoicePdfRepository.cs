
using Dapper;
using FinanceApi.Data;
using FinanceApi.ModelDTO;

namespace FinanceApi.Repositories
{
    public interface IInvoicePdfRepository
    {
        Task<InvoicePdfDto?> GetInvoiceForPdfAsync(int id);
    }

    public class InvoicePdfRepository : DynamicRepository, IInvoicePdfRepository
    {
        public InvoicePdfRepository(IDbConnectionFactory factory) : base(factory) { }

        public async Task<InvoicePdfDto?> GetInvoiceForPdfAsync(int id)
        {
            const string sql = @"
SELECT 
    si.Id,
    si.InvoiceNo,
    si.InvoiceDate,
    c.CustomerName,
    c.Email            AS CustomerEmail,
    si.Subtotal        AS SubTotal,
    si.Tax             AS TaxAmount,
    si.GrandTotal      AS GrandTotal,

    sil.ItemName,
    sil.Qty,
    sil.UnitPrice,
    sil.LineTotal
FROM SalesInvoice si
INNER JOIN Customer c       ON c.Id = si.CustomerId
INNER JOIN SalesInvoiceLine sil ON sil.SiId = si.Id
WHERE si.Id = @Id
ORDER BY sil.Id;";

            var lookup = new Dictionary<int, InvoicePdfDto>();

            await Connection.QueryAsync<InvoicePdfDto, InvoicePdfLineDto, InvoicePdfDto>(
                sql,
                (header, line) =>
                {
                    if (!lookup.TryGetValue(header.Id, out var dto))
                    {
                        dto = header;
                        dto.Lines = new List<InvoicePdfLineDto>();
                        lookup.Add(dto.Id, dto);
                    }
                    dto.Lines.Add(line);
                    return dto;
                },
                new { Id = id });

            return lookup.Values.SingleOrDefault();
        }
    }
}
