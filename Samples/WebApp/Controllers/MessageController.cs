using Microsoft.AspNetCore.Mvc;
using WebApp.DTOs;
using WebApp.Services.Interfaces;

namespace WebApp.Controllers;

[ApiController]
[Route("message")]
public class MessageController(INoteService noteService) : ControllerBase
{
    private readonly INoteService _noteService = noteService;
    
    [HttpPost]
    public async Task<IActionResult> SendMessage([FromBody] Note note)
    {
        await _noteService.SendNote(note);
        return Ok("Mensagem enviada");
    }

    [HttpPost("test-retry-publisher")]
    public async Task<IActionResult> TestRetryPublisher([FromBody] TestRetryRequest dto)
    {
        await _noteService.TestRetry(dto);
        return Ok("Mensagem enviada");
    }

    [HttpPost("test-retry-consumer")]
    public async Task<IActionResult> TestRetryConsumer([FromBody] TestRetryRequest dto)
    {
        await _noteService.TestRetryConsumer(dto);
        return Ok("Mensagem enviada");
    }
}
