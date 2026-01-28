using FinanceApi.Models;
using FinanceApi.Data;
using Microsoft.EntityFrameworkCore;
using FinanceApi.Interfaces;

namespace FinanceApi.Repositories
{
    public class PurchaseRepository : IPurchaseRepository
    {
        private readonly ApplicationDbContext _context;

        public PurchaseRepository(ApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<List<PurchasesDto>> GetAllAsync()
        {
            return await _context.Purchases
                .Include(s => s.Supplier)
                .Include(s => s.LineItems)
                .Where(c => c.IsActive)
                .OrderBy(c => c.Id)
                .Select(s => new PurchasesDto
                {
                    Id = s.Id,
                    SupplierId = s.SupplierId,
                    Date = s.Date,
                    PaymentAccount = s.PaymentAccount,
                    GrandTotal = s.GrandTotal,
                    Discount = s.Discount,
                    TotalDiscount = s.TotalDiscount,
                    NoTax = s.NoTax,
                    GstPercentage = s.GstPercentage,
                    GST = s.GST,
                    TotalTax = s.TotalTax,
                    ShippingCost = s.ShippingCost,
                    NetTotal = s.NetTotal,
                    PaidAmount = s.PaidAmount,
                    Due = s.Due,
                    Change = s.Change,
                    Details = s.Details,
                    SupplierName = s.Supplier != null ? s.Supplier.Name : null,
                    LineItems = s.LineItems,
                    CreatedBy = s.CreatedBy,
                    CreatedDate = s.CreatedDate,
                    UpdatedBy = s.UpdatedBy,
                    UpdatedDate = s.UpdatedDate,
                    IsActive = s.IsActive
                })
                .ToListAsync();
        }


        public async Task<PurchasesDto?> GetByIdAsync(int id)
        {
            return await _context.Purchases
                .Include(s => s.Supplier)
                .Include(s => s.LineItems)
                .Where(s => s.Id == id)
                .Where(c => c.IsActive)
                .Select(s => new PurchasesDto
                {
                    Id = s.Id,
                    SupplierId = s.SupplierId,
                    Date = s.Date,
                    PaymentAccount = s.PaymentAccount,
                    GrandTotal = s.GrandTotal,
                    Discount = s.Discount,
                    TotalDiscount = s.TotalDiscount,
                    NoTax = s.NoTax,
                    GstPercentage = s.GstPercentage,
                    GST = s.GST,
                    TotalTax = s.TotalTax,
                    ShippingCost = s.ShippingCost,
                    NetTotal = s.NetTotal,
                    PaidAmount = s.PaidAmount,
                    Due = s.Due,
                    Change = s.Change,
                    Details = s.Details,
                    SupplierName = s.Supplier != null ? s.Supplier.Name : null,
                    LineItems = s.LineItems,
                    CreatedBy = s.CreatedBy,
                    CreatedDate = s.CreatedDate,
                    UpdatedBy = s.UpdatedBy,
                    UpdatedDate = s.UpdatedDate,
                    IsActive = s.IsActive
                })
                .FirstOrDefaultAsync();
        }


        public async Task<Purchases> CreateAsync(Purchases purchase)
        {
            try
            {
                purchase.CreatedBy = "System";
                purchase.CreatedDate = DateTime.UtcNow;
                purchase.IsActive = true;
                _context.Purchases.Add(purchase);
                await _context.SaveChangesAsync();
                return purchase;
            }
            catch (DbUpdateException dbEx)
            {
                var innerMessage = dbEx.InnerException?.Message ?? dbEx.Message;
                throw new Exception($"Error: {innerMessage}", dbEx);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                throw;
            }
        }

        public async Task<Purchases?> UpdateAsync(int id, Purchases updatedPurchase)
        {
            try
            {
                var existingPurchase = await _context.Purchases.Include(s => s.LineItems)
                                              .FirstOrDefaultAsync(s => s.Id == id);
                if (existingPurchase == null) return null;

                // Manually update only scalar properties (excluding Id)
                existingPurchase.SupplierId = updatedPurchase.SupplierId;
                existingPurchase.Supplier = updatedPurchase.Supplier;
                existingPurchase.Date = updatedPurchase.Date;
                existingPurchase.PaymentAccount = updatedPurchase.PaymentAccount;
                existingPurchase.GrandTotal = updatedPurchase.GrandTotal;
                existingPurchase.Discount = updatedPurchase.Discount;
                existingPurchase.TotalDiscount = updatedPurchase.TotalDiscount;
                existingPurchase.NoTax = updatedPurchase.NoTax;
                existingPurchase.GstPercentage = updatedPurchase.GstPercentage;
                existingPurchase.GST = updatedPurchase.GST;
                existingPurchase.TotalTax = updatedPurchase.TotalTax;
                existingPurchase.ShippingCost = updatedPurchase.ShippingCost;
                existingPurchase.NetTotal = updatedPurchase.NetTotal;
                existingPurchase.PaidAmount = updatedPurchase.PaidAmount;
                existingPurchase.Due = updatedPurchase.Due;
                existingPurchase.Change = updatedPurchase.Change;
                existingPurchase.Details = updatedPurchase.Details;
                existingPurchase.UpdatedBy = "System";
                existingPurchase.UpdatedDate = DateTime.UtcNow;

                // Update LineItems safely
                existingPurchase.LineItems.Clear(); // Remove old items
                foreach (var item in updatedPurchase.LineItems)
                {
                    existingPurchase.LineItems.Add(item); // Add new ones
                }

                await _context.SaveChangesAsync();
                return existingPurchase;
            }
            catch (DbUpdateException dbEx)
            {
                var innerMessage = dbEx.InnerException?.Message ?? dbEx.Message;
                throw new Exception($"Error: {innerMessage}", dbEx);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                throw;
            }
        }


        public async Task<bool> DeleteAsync(int id)
        {
            var purchase = await _context.Purchases.Include(s => s.LineItems)
                                           .FirstOrDefaultAsync(s => s.Id == id);
            if (purchase == null) return false;

            purchase.IsActive = false;
            await _context.SaveChangesAsync();
            return true;
        }
    }
}
