using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    [Table("ApprovalLevel")]
    public class ApprovalLevel
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public string CreatedBy { get; set; }

        public DateTime? CreatedDate { get; set; }

        public string UpdatedBy { get; set; }

        public DateTime? UpdatedDate { get; set; }

        public bool? IsActive { get; set; }

    }
}
