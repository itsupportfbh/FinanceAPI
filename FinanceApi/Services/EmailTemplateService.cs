using System.Net;
using System.Net.Mail;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;

namespace FinanceApi.Services
{
    public class EmailTemplateService : IEmailService
    {
        private readonly SmtpEmailSettings _settings;

        public EmailTemplateService(Microsoft.Extensions.Options.IOptions<SmtpEmailSettings> options)
        {
            _settings = options.Value;
        }

        public async Task<EmailResultDto> SendInvoiceEmailAsync(EmailRequestDto dto, byte[]? pdfBytes)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(dto.ToEmail))
                    return new EmailResultDto { Success = false, Message = "ToEmail is required" };

                string fromEmail =
                    !string.IsNullOrWhiteSpace(dto.FromEmail)
                        ? dto.FromEmail
                        : (!string.IsNullOrWhiteSpace(_settings.From)
                            ? _settings.From
                            : _settings.SmtpUser);

                using var message = new MailMessage();
                message.From = new MailAddress(fromEmail, dto.FromName ?? string.Empty);
                message.To.Add(new MailAddress(dto.ToEmail, dto.ToName ?? string.Empty));

                message.Subject = string.IsNullOrWhiteSpace(dto.Subject)
                    ? "Invoice"
                    : dto.Subject;

                message.Body = string.IsNullOrWhiteSpace(dto.BodyHtml)
                    ? "<p>Please find attached invoice.</p>"
                    : dto.BodyHtml;

                message.IsBodyHtml = true;

                // ✅ PDF attachment
                if (pdfBytes != null && pdfBytes.Length > 0)
                {
                    string fileName = string.IsNullOrWhiteSpace(dto.FileName)
                        ? "Invoice.pdf"
                        : dto.FileName;

                    var stream = new MemoryStream(pdfBytes);
                    stream.Position = 0; // just to be safe

                    var attachment = new Attachment(stream, fileName, "application/pdf");
                    message.Attachments.Add(attachment);
                }

                int port = 587;
                int.TryParse(_settings.SmtpPort, out port);

                using var client = new SmtpClient(_settings.SmtpHost, port)
                {
                    EnableSsl = true,
                    Credentials = new NetworkCredential(_settings.SmtpUser, _settings.SmtpPass)
                };

                await client.SendMailAsync(message);

                return new EmailResultDto
                {
                    Success = true,
                    Message = "Email sent successfully"
                };
            }
            catch (Exception ex)
            {
                return new EmailResultDto
                {
                    Success = false,
                    Message = ex.Message
                };
            }
        }


    }
}
