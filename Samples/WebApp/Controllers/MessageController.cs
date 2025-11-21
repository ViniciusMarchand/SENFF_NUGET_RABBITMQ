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
}
