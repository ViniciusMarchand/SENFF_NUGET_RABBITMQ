using WebApp.DTOs;
using WebApp.Services.Interfaces;

namespace WebApp.Services;

public class NoteService(IRabbitMQService rabbitService) : INoteService
{
    private readonly IRabbitMQService _rabbitService = rabbitService;
    
    public async Task SendNote(Note note)
    {
        await _rabbitService.SendMessage("queue", note);
    }
}