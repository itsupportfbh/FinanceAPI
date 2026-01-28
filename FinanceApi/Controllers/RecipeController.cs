using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class RecipeController : ControllerBase
    {
        private readonly IRecipeService _srv;
        public RecipeController(IRecipeService srv) => _srv = srv;

        [HttpPost("create")]
        public async Task<IActionResult> Create([FromBody] RecipeCreateDto dto)
        {
            if (dto == null) return BadRequest("Payload required");
            if (dto.FinishedItemId <= 0) return BadRequest("FinishedItemId required");
            if (dto.Ingredients == null || dto.Ingredients.Count == 0) return BadRequest("At least 1 ingredient required");

            var user = User?.Identity?.Name ?? "system";

            try
            {
                var id = await _srv.CreateAsync(dto, user);
                return Ok(new { isSuccess = true, message = "Recipe created", id });
            }
            catch (InvalidOperationException ex)
            {
                // UNIQUE finished item -> conflict
                return Conflict(new { isSuccess = false, message = ex.Message });
            }
        }

        [HttpGet("list")]
        public async Task<IActionResult> List()
            => Ok(new { isSuccess = true, data = await _srv.ListAsync() });

        [HttpGet("{id:int}")]
        public async Task<IActionResult> GetById(int id)
        {
            var row = await _srv.GetByIdAsync(id);
            if (row == null) return NotFound(new { isSuccess = false, message = "Not found" });
            return Ok(new { isSuccess = true, data = row });
        }

        [HttpPut("{id:int}")]
        public async Task<IActionResult> Update(int id, [FromBody] RecipeUpdateDto dto)
        {
            var user = User?.Identity?.Name ?? "system";
            try
            {
                var updatedId = await _srv.UpdateAsync(id, dto, user);
                if (updatedId == 0) return NotFound(new { isSuccess = false, message = "Not found" });
                return Ok(new { isSuccess = true, message = "Updated", id = updatedId });
            }
            catch (InvalidOperationException ex)
            {
                return Conflict(new { isSuccess = false, message = ex.Message });
            }
        }

        [HttpDelete("{id:int}")]
        public async Task<IActionResult> Delete(int id)
        {
            var user = User?.Identity?.Name ?? "system";
            var ok = await _srv.DeleteAsync(id, user);
            return Ok(new { isSuccess = ok, message = ok ? "Deleted" : "Not deleted" });
        }
    }
}
