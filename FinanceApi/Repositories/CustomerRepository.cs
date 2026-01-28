using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;

namespace FinanceApi.Repositories
{
    public class CustomerRepository : ICustomerRepository
    {
        private readonly ApplicationDbContext _context;

        public CustomerRepository(ApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<List<CustomerDto>> GetAllAsync()
        {
            var result = await _context.Customer
                .Include(c => c.City)
                .Include(c => c.State)
                .Include(c => c.Country)
                .Include(c => c.CustomerGroups)
                .Include(c => c.Region)
                .Where(c => c.IsActive)
                .OrderBy(c => c.Id)
                .Select(c => new CustomerDto
                {
                    Id = c.Id,
                    Name = c.Name,
                    ContactName = c.ContactName,
                    ContactTitle = c.ContactTitle,
                    CityId = c.CityId,
                    CityName = c.City !=null ? c.City.CityName : string.Empty,
                    StateId = c.StateId,
                    StateName = c.State != null ? c.State.StateName : string.Empty,
                    CountryId = c.CountryId,
                    CountryName = c.Country != null ? c.Country.CountryName : string.Empty,
                    CustomerGroupId = c.CustomerGroupId,
                    CustomerGroupName = c.CustomerGroups != null ? c.CustomerGroups.Name : string.Empty,
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

        public async Task<CustomerDto?> GetByIdAsync(int id)
        {
            var result = await _context.Customer
                .Include(c => c.City)
                .Include(c => c.State)
                .Include(c => c.Country)
                .Include(c => c.CustomerGroups)
                .Include(c => c.Region)
                .Where(c => c.Id == id)
                .Where(c => c.IsActive)
                .Select(c => new CustomerDto
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
                    CustomerGroupId = c.CustomerGroupId,
                    CustomerGroupName = c.CustomerGroups != null ? c.CustomerGroups.Name : string.Empty,
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

        public async Task<Customer> CreateAsync(Customer customer)
        {
            try
            {
                customer.CreatedBy = "System";
                customer.CreatedDate = DateTime.UtcNow;
                customer.IsActive = true;
                _context.Customer.Add(customer);
                await _context.SaveChangesAsync();
                return customer;
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

        public async Task<Customer?> UpdateAsync(int id, Customer updatedCustomer)
        {
            try
            {
                var existingCustomer = await _context.Customer.FirstOrDefaultAsync(s => s.Id == id);
                if (existingCustomer == null) return null;

                // Manually update only scalar properties (excluding Id)
                existingCustomer.Name = updatedCustomer.Name;
                existingCustomer.ContactName = updatedCustomer.ContactName;
                existingCustomer.ContactTitle = updatedCustomer.ContactTitle;
                existingCustomer.CityId = updatedCustomer.CityId;
                existingCustomer.StateId = updatedCustomer.StateId;
                existingCustomer.CountryId = updatedCustomer.CountryId;
                existingCustomer.CustomerGroupId = updatedCustomer.CustomerGroupId;
                existingCustomer.RegionId = updatedCustomer.RegionId;
                existingCustomer.Address = updatedCustomer.Address;
                existingCustomer.PostalCode = updatedCustomer.PostalCode;
                existingCustomer.Phone = updatedCustomer.Phone;
                existingCustomer.Website = updatedCustomer.Website;
                existingCustomer.Fax = updatedCustomer.Fax;
                existingCustomer.Email = updatedCustomer.Email;
                existingCustomer.UpdatedBy = "System";
                existingCustomer.UpdatedDate = DateTime.UtcNow;

                await _context.SaveChangesAsync();
                return existingCustomer;

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
            var customer = await _context.Customer.FirstOrDefaultAsync(s => s.Id == id);
            if (customer == null) return false;

            customer.IsActive = false;
            await _context.SaveChangesAsync();
            return true;
        }
    }
}
