using System.Net.Mail;
using System.Net;
using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class EmailService : IEmailService
    {
        private readonly IConfiguration _config;

        public EmailService(IConfiguration config)
        {
            _config = config;
        }

        public async Task SendResetPasswordEmail(User user, string resetLink)
        {
            try
            {
                var fromEmail = _config["EmailSettings:From"];
                var smtpHost = _config["EmailSettings:SmtpHost"];
                var smtpPort = int.Parse(_config["EmailSettings:SmtpPort"]);
                var smtpUser = _config["EmailSettings:SmtpUser"];
                var smtpPass = _config["EmailSettings:SmtpPass"];

                var subject = "Reset Your Password";
                var body = $@"
                    <p>Hello,{user.Username}</p>
                    <p>You requested a password reset. Click the link below to reset your password:</p>
                    <p><a href='{resetLink}'>Reset Password</a></p>
                    <p>This link will expire in 1 hour.</p>
                ";

                using var message = new MailMessage(fromEmail, user.Email, subject, body)
                {
                    IsBodyHtml = true
                };

                using var client = new SmtpClient(smtpHost, smtpPort)
                {
                    Credentials = new NetworkCredential(smtpUser, smtpPass),
                    EnableSsl = true
                };

                await client.SendMailAsync(message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Email sending failed: {ex.Message}");
                throw;
            }
        }


        public async Task SendUsernameEmail(User user)
        {
            try
            {
                var fromEmail = _config["EmailSettings:From"];
                var smtpHost = _config["EmailSettings:SmtpHost"];
                var smtpPort = int.Parse(_config["EmailSettings:SmtpPort"]);
                var smtpUser = _config["EmailSettings:SmtpUser"];
                var smtpPass = _config["EmailSettings:SmtpPass"];

                var subject = "Your Username";
                var body = $@"
                    <p>Hello</p>
                    <p>You requested your Unity_ERP username. Use the username to proceed further:</p>
                    <p> Your username: {user.Username}</p>
                    <p>If you didn’t request this, you can ignore this email.</p>
                ";

                using var message = new MailMessage(fromEmail, user.Email, subject, body)
                {
                    IsBodyHtml = true
                };

                using var client = new SmtpClient(smtpHost, smtpPort)
                {
                    Credentials = new NetworkCredential(smtpUser, smtpPass),
                    EnableSsl = true
                };

                await client.SendMailAsync(message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Email sending failed: {ex.Message}");
                throw;
            }
        }
        public async Task SendSupplierPoEmailAsync(
    string supplierEmail,
    string supplierName,
    string poNo,
    byte[] pdfBytes)
        {
            var fromEmail = _config["EmailSettings:From"];
            var smtpHost = _config["EmailSettings:SmtpHost"];
            var smtpPort = int.Parse(_config["EmailSettings:SmtpPort"]);
            var smtpUser = _config["EmailSettings:SmtpUser"];
            var smtpPass = _config["EmailSettings:SmtpPass"];

            var subject = $"Purchase Order {poNo}";
            var body = $@"
        <p>Hello {supplierName},</p>
        <p>Please find attached Purchase Order <b>{poNo}</b>.</p>
        <p>Regards,<br/>CSPL</p>";

            using var message = new MailMessage(fromEmail, supplierEmail, subject, body)
            {
                IsBodyHtml = true
            };

            message.Attachments.Add(
                new Attachment(
                    new MemoryStream(pdfBytes),
                    $"{poNo}.pdf",
                    "application/pdf"
                )
            );

            using var client = new SmtpClient(smtpHost, smtpPort)
            {
                Credentials = new NetworkCredential(smtpUser, smtpPass),
                EnableSsl = true
            };

            await client.SendMailAsync(message);
        }

    }
}
