using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IEmailRepository
    {
        Task<EmailTemplateDto?> GetTemplateAsync(int id, string? docType = null);

        Task<IEnumerable<EmailInvoiceListItemDto>> GetInvoiceListAsync(string docType);

        Task<EmailInvoiceInfoDto?> GetInvoiceInfoAsync(string docType, int invoiceId);
    }
}
