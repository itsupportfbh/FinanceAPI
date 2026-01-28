using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Connections;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class EmailTemplateRepository: DynamicRepository,IEmailRepository
    {
        public EmailTemplateRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        public async Task<EmailTemplateDto?> GetTemplateAsync(int id, string? docType = null)
        {
           

            const string sql = @"
SELECT 
    Id,
    TemplateName,
    SubjectTemplate,
    BodyTemplate,
    IsActive
FROM dbo.EmailTemplate
WHERE Id = @Id
  AND IsActive = 1;

";

            return await Connection.QueryFirstOrDefaultAsync<EmailTemplateDto>(
                sql,
                new { Id = id, DocType = docType }
            );
        }

        public async Task<IEnumerable<EmailInvoiceListItemDto>> GetInvoiceListAsync(string docType)
        {
           

            if (docType == "SI")
            {
                const string sqlSi = @"
SELECT 
    si.Id,
    si.InvoiceNo,
    ISNULL(c.CustomerName, '') AS PartyName
FROM dbo.SalesInvoice si
LEFT JOIN dbo.SalesOrder so 
    ON so.Id = si.SoId
LEFT JOIN dbo.Customer c 
    ON c.Id = so.CustomerId
WHERE si.IsActive = 1
  AND LTRIM(RTRIM(ISNULL(si.InvoiceNo, ''))) <> ''   
ORDER BY si.InvoiceDate DESC, si.InvoiceNo DESC;
";
                return await Connection.QueryAsync<EmailInvoiceListItemDto>(sqlSi);
            }
            else if (docType == "PIN")
            {
                const string sqlPin = @"
SELECT 
    p.Id,
    p.InvoiceNo,
    ISNULL(s.Name, '') AS PartyName
FROM dbo.SupplierInvoicePin p
LEFT JOIN dbo.Suppliers s ON s.Id = p.SupplierId
WHERE p.IsActive = 1
ORDER BY p.InvoiceDate DESC, p.InvoiceNo DESC;
";
                return await Connection.QueryAsync<EmailInvoiceListItemDto>(sqlPin);
            }
            else
            {
                return Enumerable.Empty<EmailInvoiceListItemDto>();
            }
        }

        public async Task<EmailInvoiceInfoDto?> GetInvoiceInfoAsync(string docType, int invoiceId)
        {
            

            if (docType == "SI")
            {
                const string sqlSi = @"
SELECT 
    si.Id,
    si.InvoiceNo,
    ISNULL(c.CustomerName, '') AS PartyName,
    -- choose the email you want to use (AP email, billing email, etc.)
    ISNULL(NULLIF(c.Email, ''), c.Email) AS Email,
   
    si.Total AS Amount
FROM dbo.SalesInvoice si
LEFT JOIN dbo.SalesOrder so 
    ON so.Id = si.SoId      -- 🔁 link to SalesOrder
LEFT JOIN dbo.Customer c 
    ON c.Id = so.CustomerId         -- 🔁 customer from SalesOrder
WHERE si.Id =  @InvoiceId;
";
                return await Connection.QueryFirstOrDefaultAsync<EmailInvoiceInfoDto>(
                    sqlSi,
                    new { InvoiceId = invoiceId }
                );
            }
            else if (docType == "PIN")
            {
                const string sqlPin = @"
SELECT 
    p.Id,
    p.InvoiceNo,
    ISNULL(s.Name, '') AS PartyName,
    ISNULL(NULLIF(s.Email, ''), s.Email) AS Email
    
FROM dbo.SupplierInvoicePin p
LEFT JOIN dbo.Suppliers s ON s.Id = p.SupplierId
WHERE p.Id =@InvoiceId;
";
                return await Connection.QueryFirstOrDefaultAsync<EmailInvoiceInfoDto>(
                    sqlPin,
                    new { InvoiceId = invoiceId }
                );
            }
            else
            {
                return null;
            }
        }
    }
}
