using Microsoft.EntityFrameworkCore;
using FinanceApi.Models;

namespace FinanceApi.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }

        // Add DbSet properties for each entity

        public DbSet<User> User { get; set; }
        public DbSet<PasswordHistory> PasswordHistory { get; set; }
        public DbSet<Country> Country { get; set; }
        public DbSet<State> State { get; set; }
        public DbSet<City> City { get; set; }
        public DbSet<Service> Service { get; set; }
        public DbSet<CustomerGroups> CustomerGroups { get; set; }
        public DbSet<SupplierGroups> SupplierGroups { get; set; }
        public DbSet<Region> Region { get; set; }
        public DbSet<Location> Location { get; set; }
        public DbSet<Warehouse> Warehouse { get; set; }
        public DbSet<Deduction> Deduction { get; set; }
        public DbSet<Income> Income { get; set; }
        public DbSet<Department> Department { get; set; }
        public DbSet<Uom> Uom { get; set; }
        public DbSet<Item> Item { get; set; }
        public DbSet<Customer> Customer { get; set; }
        public DbSet<Supplier> Supplier { get; set; }
        public DbSet<Sales> Sales { get; set; }
        public DbSet<Purchases> Purchases { get; set; }
        public DbSet<SaleLineItems> SaleLineItems { get; set; }
        public DbSet<PurchaseLineItem> PurchaseLineItems { get; set; }
        public DbSet<Meeting> Meetings { get; set; }
        public DbSet<Attendees> Attendees { get; set; }
        public DbSet<PasswordResetToken> PasswordResetToken { get; set; }
        public DbSet<ChartOfAccount> ChartOfAccount { get; set; }
        //public DbSet<OpeningBalance> OpeningBalance { get; set; }

        public DbSet<PurchaseRequest> PurchaseRequests { get; set; }
        public DbSet<PurchaseRequestTemp> PurchaseRequestsTemp { get; set; }
        public DbSet<PurchaseOrder> PurchaseOrder { get; set; }

        public DbSet<ApprovalLevel> ApprovalLevels { get; set; }

        public DbSet<PaymentTerms> PaymentTerms { get; set; }

        public DbSet<Currency> Currencies { get; set; }

        public DbSet<Suppliers> suppliers { get; set; }
        public DbSet<ApprovalLevel> ApprovalLevel { get; set; }
        public DbSet<UserApprovalLevel> UserApprovalLevel { get; set; }
        public DbSet<OcrDocument> OcrDocuments { get; set; }
        public DbSet<SupplierInvoicePin> SupplierInvoicePin { get; set; }


        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // Configure Sale-SaleLineItem relationship
            modelBuilder.Entity<Sales>()
                .HasMany(s => s.LineItems)
                .WithOne()
                .OnDelete(DeleteBehavior.Cascade);

            // Configure Purchase-PurchaseLineItem relationship
            modelBuilder.Entity<Purchases>()
                .HasMany(p => p.LineItems)
                .WithOne()
                .OnDelete(DeleteBehavior.Cascade);

            // ✅ USER ↔ APPROVALLEVEL (Many-to-Many via UserApprovalLevel)
            modelBuilder.Entity<UserApprovalLevel>(entity =>
            {
                entity.ToTable("UserApprovalLevel"); // ✅ table name must match DB

                entity.HasKey(x => new { x.UserId, x.ApprovalLevelId }); // ✅ composite PK

                entity.HasOne(x => x.User)
                      .WithMany() // if you DID NOT add navigation in User
                      .HasForeignKey(x => x.UserId)
                      .OnDelete(DeleteBehavior.Cascade);

                entity.HasOne(x => x.ApprovalLevel)
                      .WithMany() // if you DID NOT add navigation in ApprovalLevel
                      .HasForeignKey(x => x.ApprovalLevelId)
                      .OnDelete(DeleteBehavior.Cascade);
            });

            // (optional) if you want PasswordResetToken relationship back, uncomment your block
        }


    }
}
