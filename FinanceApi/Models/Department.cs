namespace FinanceApi.Models
{
    public class Department : BaseEntity
    {
        public int Id { get; set; }
        public string DepartmentCode { get; set; }

        public string DepartmentName { get; set; }
    }
}