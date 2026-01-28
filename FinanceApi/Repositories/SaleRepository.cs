using FinanceApi.Models;
using FinanceApi.Data;
using Microsoft.EntityFrameworkCore;

namespace FinanceApi.Repositories
{
    public class SaleRepository : ISaleRepository
    {
        private readonly ApplicationDbContext _context;

        public SaleRepository(ApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<List<SalesDto>> GetAllAsync()
        {
            return await _context.Sales
                .Include(s => s.Customer)
                .Include(s => s.LineItems)
                .Where(c => c.IsActive)
                .OrderBy(c => c.Id)
                .Select(s => new SalesDto
                {
                    Id = s.Id,
                    CustomerId = s.CustomerId,               
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
                    CustomerName = s.Customer != null ? s.Customer.Name : null,
                    LineItems = s.LineItems,
                    CreatedBy = s.CreatedBy,
                    CreatedDate = s.CreatedDate,
                    UpdatedBy = s.UpdatedBy,
                    UpdatedDate = s.UpdatedDate,
                    IsActive = s.IsActive
                })
                .ToListAsync();
        }


        public async Task<SalesDto?> GetByIdAsync(int id)
        {
            return await _context.Sales
                .Include(s => s.Customer)
                .Include(s => s.LineItems)
                .Where(s => s.Id == id)
                .Where(c => c.IsActive)
                .Select(s => new SalesDto
                {
                    Id = s.Id,
                    CustomerId = s.CustomerId,
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
                    CustomerName = s.Customer != null ? s.Customer.Name : null,
                    LineItems = s.LineItems,
                    CreatedBy = s.CreatedBy,
                    CreatedDate = s.CreatedDate,
                    UpdatedBy = s.UpdatedBy,
                    UpdatedDate = s.UpdatedDate,
                    IsActive = s.IsActive
                })
                .FirstOrDefaultAsync();
        }


        public async Task<Sales> CreateAsync(Sales sale)
        {
            try
            {
                sale.CreatedBy = "System";
                sale.CreatedDate = DateTime.UtcNow;
                sale.IsActive = true;
                _context.Sales.Add(sale);
                await _context.SaveChangesAsync();
                return sale;

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

        public async Task<Sales?> UpdateAsync(int id, Sales updatedSale)
        {
            try
            {
                var existingSale = await _context.Sales.Include(s => s.LineItems)
                                                   .FirstOrDefaultAsync(s => s.Id == id);
                if (existingSale == null) return null;

                // Manually update only scalar properties (excluding Id)
                existingSale.CustomerId = updatedSale.CustomerId;
                existingSale.Customer = updatedSale.Customer;
                existingSale.Date = updatedSale.Date;
                existingSale.PaymentAccount = updatedSale.PaymentAccount;
                existingSale.GrandTotal = updatedSale.GrandTotal;
                existingSale.Discount = updatedSale.Discount;
                existingSale.TotalDiscount = updatedSale.TotalDiscount;
                existingSale.NoTax = updatedSale.NoTax;
                existingSale.GstPercentage = updatedSale.GstPercentage;
                existingSale.GST = updatedSale.GST;
                existingSale.TotalTax = updatedSale.TotalTax;
                existingSale.ShippingCost = updatedSale.ShippingCost;
                existingSale.NetTotal = updatedSale.NetTotal;
                existingSale.PaidAmount = updatedSale.PaidAmount;
                existingSale.Due = updatedSale.Due;
                existingSale.Change = updatedSale.Change;
                existingSale.Details = updatedSale.Details;
                existingSale.UpdatedBy = "System";
                existingSale.UpdatedDate = DateTime.UtcNow;

                // Update LineItems safely
                existingSale.LineItems.Clear(); // Remove old items
                foreach (var item in updatedSale.LineItems)
                {
                    existingSale.LineItems.Add(item); // Add new ones
                }

                await _context.SaveChangesAsync();
                return existingSale;
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
            var sale = await _context.Sales.Include(s => s.LineItems)
                                           .FirstOrDefaultAsync(s => s.Id == id);
            if (sale == null) return false;

            sale.IsActive = false;
            await _context.SaveChangesAsync();
            return true;
        }
    }
}

