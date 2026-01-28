// Services/SmtpEmailService.cs
using System.Net;
using System.Net.Mail;
using FinanceApi.ModelDTO;
using Microsoft.Extensions.Options;

namespace FinanceApi.Services
{
    public class EmailSettings
    {
        public string From { get; set; } = "";
        public string SmtpHost { get; set; } = "";
        public int SmtpPort { get; set; }
        public string SmtpUser { get; set; } = "";
        public string SmtpPass { get; set; } = "";
    }

    public interface ISmtEmailService
    {
        Task<EmailResultDto> SendInvoiceEmailAsync(EmailRequestDto dto, byte[] pdfBytes);
    }

    public class SmtEmailService : ISmtEmailService
    {
        private readonly EmailSettings _settings;

        public SmtEmailService(IOptions<EmailSettings> options)
        {
            _settings = options.Value;
        }

        public async Task<EmailResultDto> SendInvoiceEmailAsync(EmailRequestDto dto, byte[] pdfBytes)
        {
            using var msg = new MailMessage();

            msg.From = new MailAddress(
                string.IsNullOrWhiteSpace(dto.FromEmail) ? _settings.From : dto.FromEmail,
                string.IsNullOrWhiteSpace(dto.FromName) ? dto.FromEmail : dto.FromName
            );

            msg.To.Add(new MailAddress(dto.ToEmail, dto.ToName));
            msg.Subject = dto.Subject;
            msg.Body = dto.BodyHtml;
            msg.IsBodyHtml = true;

            // attach generated PDF
            var fileName = string.IsNullOrWhiteSpace(dto.FileName) ? "Invoice.pdf" : dto.FileName;
            msg.Attachments.Add(new Attachment(new MemoryStream(pdfBytes), fileName, "application/pdf"));

            using var client = new SmtpClient(_settings.SmtpHost, _settings.SmtpPort)
            {
                Credentials = new NetworkCredential(_settings.SmtpUser, _settings.SmtpPass),
                EnableSsl = true
            };

            await client.SendMailAsync(msg);

            return new EmailResultDto
            {
                Success = true,
                Message = "Invoice email sent."
            };
        }
    }
}
