using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;



namespace FinanceApi.Repositories
{
    public class SupplierRepository : ISupplierRepository
    {
        private readonly ApplicationDbContext _context;

        public SupplierRepository(ApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<List<SupplierDto>> GetAllAsync()
        {
            var result = await _context.Supplier
                .Include(c => c.City)
                .Include(c => c.State)
                .Include(c => c.Country)
                .Include(c => c.SupplierGroups)
                .Include(c => c.Region)
                .Where(c => c.IsActive)
                .OrderBy(c => c.Id)
                .Select(c => new SupplierDto
                {
                    Id = c.Id,
                    Name = c.Name,
                    ContactName = c.ContactName,
                    ContactTitle = c.ContactTitle,
                    CityId = c.CityId,
                    CityName = c.City != null ? c.City.CityName : string.Empty,
                    StateId = c.StateId,
                    StateName = c.State != null ? c.State.StateName : string.Empty,
                    CountryId = c.CountryId,
                    CountryName = c.Country != null ? c.Country.CountryName : string.Empty,
                    SupplierGroupId = c.SupplierGroupId,
                    SupplierGroupName = c.SupplierGroups != null ? c.SupplierGroups.Name : string.Empty,
                    RegionId = c.RegionId,
                    RegionName = c.Region != null ? c.Region.RegionName : string.Empty,
                    Address = c.Address,
                    PostalCode = c.PostalCode,
                    Phone = c.Phone,
                    Website = c.Website,
                    Fax = c.Fax,
                    Email = c.Email,
                    CreatedBy = c.CreatedBy,
                    CreatedDate = c.CreatedDate,
                    UpdatedBy = c.UpdatedBy,
                    UpdatedDate = c.UpdatedDate,
                    IsActive = c.IsActive

                })
                .ToListAsync();
            return result;
        }

        public async Task<SupplierDto?> GetByIdAsync(int id)
        {
            var result = await _context.Supplier
                .Include(c => c.City)
                .Include(c => c.State)
                .Include(c => c.Country)
                .Include(c => c.SupplierGroups)
                .Include(c => c.Region)
                .Where(c => c.Id == id)
                .Where(c => c.IsActive)
                .Select(c => new SupplierDto
                {
                    Id = c.Id,
                    Name = c.Name,
                    ContactName = c.ContactName,
                    ContactTitle = c.ContactTitle,
                    CityId = c.CityId,
                    CityName = c.City != null ? c.City.CityName : string.Empty,
                    StateId = c.StateId,
                    StateName = c.State != null ? c.State.StateName : string.Empty,
                    CountryId = c.CountryId,
                    CountryName = c.Country != null ? c.Country.CountryName : string.Empty,
                    SupplierGroupId = c.SupplierGroupId,
                    SupplierGroupName = c.SupplierGroups != null ? c.SupplierGroups.Name : string.Empty,
                    RegionId = c.RegionId,
                    RegionName = c.Region != null ? c.Region.RegionName : string.Empty,
                    Address = c.Address,
                    PostalCode = c.PostalCode,
                    Phone = c.Phone,
                    Website = c.Website,
                    Fax = c.Fax,
                    Email = c.Email,
                    CreatedBy = c.CreatedBy,
                    CreatedDate = c.CreatedDate,
                    UpdatedBy = c.UpdatedBy,
                    UpdatedDate = c.UpdatedDate,
                    IsActive = c.IsActive

                })
                .FirstOrDefaultAsync();
            return result;
        }

        public async Task<Supplier> CreateAsync(Supplier supplier)
        {
            

            try
            {
                supplier.CreatedBy = "System";
                supplier.CreatedDate = DateTime.UtcNow;
                supplier.IsActive = true;
                _context.Supplier.Add(supplier);
                await _context.SaveChangesAsync();
                return supplier;
            }
            catch (DbUpdateException dbEx)
            {
             
                var innerMessage = dbEx.InnerException?.Message ?? dbEx.Message;

                throw new Exception($"Error saving supplier: {innerMessage}", dbEx);
            }
            catch (Exception ex)
            {
                
                Console.WriteLine($"General error: {ex.Message}");
                throw;
            }
        }

        public async Task<Supplier?> UpdateAsync(int id, Supplier updatedSupplier)
        {
            try
            {
                var existingSupplier = await _context.Supplier.FirstOrDefaultAsync(s => s.Id == id);
                if (existingSupplier == null) return null;

                // Manually update only scalar properties (excluding Id)
                existingSupplier.Name = updatedSupplier.Name;
                existingSupplier.ContactName = updatedSupplier.ContactName;
                existingSupplier.ContactTitle = updatedSupplier.ContactTitle;
                existingSupplier.CityId = updatedSupplier.CityId;
                existingSupplier.StateId = updatedSupplier.StateId;
                existingSupplier.CountryId = updatedSupplier.CountryId;
                existingSupplier.SupplierGroupId = updatedSupplier.SupplierGroupId;
                existingSupplier.RegionId = updatedSupplier.RegionId;
                existingSupplier.Address = updatedSupplier.Address;
                existingSupplier.PostalCode = updatedSupplier.PostalCode;
                existingSupplier.Phone = updatedSupplier.Phone;
                existingSupplier.Website = updatedSupplier.Website;
                existingSupplier.Fax = updatedSupplier.Fax;
                existingSupplier.Email = updatedSupplier.Email;
                existingSupplier.UpdatedBy = "System";
                existingSupplier.UpdatedDate = DateTime.UtcNow;

                await _context.SaveChangesAsync();
                return existingSupplier;
            }
            catch (DbUpdateException dbEx)
            {

                var innerMessage = dbEx.InnerException?.Message ?? dbEx.Message;

                throw new Exception($"Error saving supplier: {innerMessage}", dbEx);
            }
            catch (Exception ex)
            {

                Console.WriteLine($"General error: {ex.Message}");
                throw;
            }
        }


        public async Task<bool> DeleteAsync(int id)
        {
            var supplier = await _context.Supplier.FirstOrDefaultAsync(s => s.Id == id);
            if (supplier == null) return false;

            supplier.IsActive = false;
            await _context.SaveChangesAsync();
            return true;
        }
    }
}
