// FinanceApi/InterfaceService/IPdfService.cs
using System.Threading.Tasks;

namespace FinanceApi.InterfaceService
{
    public interface IPdfService
    {
        // Sales Invoice (SI)
        Task<byte[]> GenerateSalesInvoicePdfAsync(int salesInvoiceId);

        // Supplier Invoice (PIN)
        Task<byte[]> GenerateSupplierInvoicePdfAsync(int supplierInvoiceId);
    }
}
