using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace FinanceApi.ModelDTO
{
    // -------- Reorder Suggestion Payloads (from UI) --------

    public record ReorderSuggestionLineDto(
        long ItemId,
        decimal Qty,
        decimal Price,
        string? Location,
        string? ItemName,
        DateTime? DeliveryDate // optional per-line date
    );

    public record ReorderSuggestionGroupDto(
        long SupplierId,
        long WarehouseId,
        List<ReorderSuggestionLineDto> Lines
    );

    /// <summary>
    /// Request coming from the UI to create PRs from reorder suggestions.
    /// Includes optional header-level DeliveryDate (fallback: earliest line date; else tomorrow).
    /// </summary>
    public record CreateReorderSuggestionsRequest(
        List<ReorderSuggestionGroupDto> Groups,
        string? Note,
        long UserId,
        string? UserName,
        long? DepartmentId,
        DateTime? DeliveryDate, // optional header-level date
        long StockReorderId
    );

    // -------- Service/Response DTOs --------

    public sealed class CreatedPrDto
    {
        public int Id { get; set; }
        public string PurchaseRequestNo { get; set; } = "";
    }

    /// <summary>
    /// Item meta loaded server-side to enrich PR lines (code, uom, budget, etc).
    /// </summary>
    public sealed class ItemMeta
    {
        public long ItemId { get; set; }
        public string Sku { get; set; } = "";
        public string Name { get; set; } = "";
        public string Uom { get; set; } = "";
        public string? Budget { get; set; } // e.g., ChartOfAccount.HeadName
        public long? BudgetHeadId { get; set; } // optional FK if you need it
    }
}
