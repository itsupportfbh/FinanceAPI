using FinanceApi.ModelDTO;

namespace FinanceApi.InterfaceService
{
    public interface IEmailService
    {
        Task<EmailResultDto> SendInvoiceEmailAsync(EmailRequestDto dto, byte[]? pdfBytes);

    }
}
