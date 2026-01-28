using FinanceApi.Models;

namespace FinanceApi.ModelDTO
{
    public class CustomerGroupsDTO : BaseEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }
}
