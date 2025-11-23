using WebApp.Services.Interfaces;
using System.Threading.Channels;
using WebApp.DTOs;

namespace WebApp.Services;
public class RabbitMQListener(IRabbitMQService rabbitService) : IListenerService
{
    private readonly IRabbitMQService _rabbitService = rabbitService;

    public void Start(string queue)
    {
        Console.WriteLine($"Iniciando listener na fila {queue}");

        _rabbitService.StartConsumer<Note>(queue, async msg =>
        {
            Console.WriteLine($"Mensagem recebida: {msg}");
        });
    }
}