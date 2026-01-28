using Microsoft.AspNetCore.Http;

namespace FinanceApi.ModelDTO
{
    public class EmailSupplierPoRequest
    {
        public IFormFile Pdf { get; set; } = default!;
    }
}
