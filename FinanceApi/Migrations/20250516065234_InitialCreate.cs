using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace FinanceApi.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Sales",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Customer = table.Column<string>(type: "text", nullable: false),
                    Date = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    PaymentAccount = table.Column<string>(type: "text", nullable: false),
                    GrandTotal = table.Column<decimal>(type: "numeric", nullable: false),
                    Discount = table.Column<decimal>(type: "numeric", nullable: false),
                    TotalDiscount = table.Column<decimal>(type: "numeric", nullable: false),
                    NoTax = table.Column<decimal>(type: "numeric", nullable: false),
                    GST = table.Column<decimal>(type: "numeric", nullable: false),
                    TotalTax = table.Column<decimal>(type: "numeric", nullable: false),
                    ShippingCost = table.Column<decimal>(type: "numeric", nullable: false),
                    NetTotal = table.Column<decimal>(type: "numeric", nullable: false),
                    PaidAmount = table.Column<decimal>(type: "numeric", nullable: false),
                    Due = table.Column<decimal>(type: "numeric", nullable: false),
                    Change = table.Column<decimal>(type: "numeric", nullable: false),
                    Details = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Sales", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "SaleLineItems",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    ProductName = table.Column<string>(type: "text", nullable: false),
                    Description = table.Column<string>(type: "text", nullable: false),
                    UnitName = table.Column<string>(type: "text", nullable: false),
                    Quantity = table.Column<int>(type: "integer", nullable: false),
                    Rate = table.Column<decimal>(type: "numeric", nullable: false),
                    Total = table.Column<decimal>(type: "numeric", nullable: false),
                    Dicount = table.Column<decimal>(type: "numeric", nullable: false),
                    SaleId = table.Column<int>(type: "integer", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SaleLineItems", x => x.Id);
                    table.ForeignKey(
                        name: "FK_SaleLineItems_Sales_SaleId",
                        column: x => x.SaleId,
                        principalTable: "Sales",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_SaleLineItems_SaleId",
                table: "SaleLineItems",
                column: "SaleId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "SaleLineItems");

            migrationBuilder.DropTable(
                name: "Sales");
        }
    }
}
