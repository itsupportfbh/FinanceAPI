public class BankDto
{
    public int Id { get; set; }

    public string BankName { get; set; }

    public string AccountHolderName { get; set; }

    public long AccountNo { get; set; } 

    public int AccountType { get; set; }

    public string Branch { get; set; }

    public string IFSC { get; set; }

    public string Routing { get; set; }

    public int CurrencyId { get; set; }

    public int CountryId { get; set; }

    public string PrimaryContact { get; set; }

    public string Email { get; set; }

    public string ContactNo { get; set; }

    public string Address { get; set; }
    public bool IsActive { get; set; }
    public int BudgetLineId {  get; set; }  
}
