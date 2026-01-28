using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IEmailService
    {
        Task SendResetPasswordEmail(User toEmail, string resetLink);
        Task SendUsernameEmail(User user);

        Task SendSupplierPoEmailAsync(
          string supplierEmail,
          string supplierName,
          string poNo,
          byte[] pdfBytes
      );
    }
}
