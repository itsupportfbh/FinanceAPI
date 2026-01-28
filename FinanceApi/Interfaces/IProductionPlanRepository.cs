using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Interfaces
{
    public interface IProductionPlanRepository
    {
        Task<IEnumerable<SoHeaderDto>> GetSalesOrdersAsync(int? includeSoId = null);
        Task<ProductionPlanResponseDto> GetBySalesOrderAsync(int salesOrderId, int warehouseId);
        Task<int> SavePlanAsync(SavePlanRequest req);
        Task<List<ProductionPlanListDto>> ListPlansWithLinesAsync();

        Task<IEnumerable<ShortageGrnAlertDto>> GetShortageGrnAlertsAsync();
        Task<int> UpdateAsync(ProductionPlanUpdateRequest req);
        Task<int> DeleteAsync(int id);
        Task<ProductionPlanGetByIdDto> GetByIdAsync(int id);

    }
}
