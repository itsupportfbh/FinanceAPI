// Controllers/QuotationController.cs
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("api/[controller]")]
public class QuotationController : ControllerBase
{
    private readonly IQuotationService _svc;
    public QuotationController(IQuotationService svc) { _svc = svc; }

    [HttpGet("GetAll")]
    public async Task<IActionResult> GetAll()
    {
        var list = await _svc.GetAllAsync();
        return Ok(new ResponseResult(true, "Success", list));
    }

    [HttpGet("GetById/{id}")]
    public async Task<IActionResult> GetById(int id)
    {
        var q = await _svc.GetByIdAsync(id);
        if (q is null) return Ok(new ResponseResult(false, "Not found", null));
        return Ok(new ResponseResult(true, "Success", q));
    }

    [HttpPost("Create")]
    public async Task<IActionResult> Create([FromBody] QuotationDTO dto)
    {
        var userId = 1; // from auth in real life
        var id = await _svc.CreateAsync(dto, userId);
        return Ok(new ResponseResult(true, "Created", id));
    }

    [HttpPut("Update/{id}")]
    public async Task<IActionResult> Update(int id, [FromBody] QuotationDTO dto)
    {
        dto.Id = id;
        var userId = 1;
        await _svc.UpdateAsync(dto, userId);
        return Ok(new ResponseResult(true, "Updated", null));
    }

    [HttpDelete("Delete/{id}")]
    public async Task<IActionResult> Delete(int id)
    {
        var userId = 1;
        await _svc.DeleteAsync(id, userId);
        return Ok(new ResponseResult(true, "Deleted", null));
    }

}
