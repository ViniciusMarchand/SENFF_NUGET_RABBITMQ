using System.Text;
using System.Text.Json;
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

    public async Task TestRetry(TestRetryRequest dto)
    {
        Console.WriteLine("Bloqueando permissão de write...");
        await SetRabbitPermission(false);

        _ = Task.Run(async () =>
        {
            await Task.Delay(dto.TimeWithErrorMs);
            Console.WriteLine("Restaurando permissão...");
            await SetRabbitPermission(true);
        });

        try
        {
            await _rabbitService.SendMessage("queue", dto.Note, dto.RetryQuantity);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Publicação falhou mesmo com retry: " + ex.Message);
        }
    }

    public async Task     TestRetryConsumer(TestRetryRequest dto)
    {
        Console.WriteLine("Bloqueando permissão...");
        await SetRabbitPermission(false);

        _ = Task.Run(async () =>
        {
            await Task.Delay(dto.TimeWithErrorMs);
            Console.WriteLine("Restaurando permissão...");
            await SetRabbitPermission(true);
        });
        
        _rabbitService.StartConsumer<Note>("teste", async msg =>
        {
            Console.WriteLine($"Mensagem recebida no TestRetryConsumer: {msg}");
            await Task.CompletedTask;
        }, dto.RetryQuantity);

        await _rabbitService.SendMessage("teste", dto.Note, 20);
    }

    private static async Task SetRabbitPermission(bool allowWrite)
    {
        var client = new HttpClient();
        var url = "http://host.docker.internal:15672/api/permissions/%2F/guest";

        var body = new
        {
            configure = allowWrite ? ".*" : "",
            write = allowWrite ? ".*" : "",
            read = allowWrite ? ".*" : ""
        };

        var json = JsonSerializer.Serialize(body);

        var req = new HttpRequestMessage(HttpMethod.Put, url)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };

        var authToken = Convert.ToBase64String(Encoding.UTF8.GetBytes("guest:guest"));
        req.Headers.Add("Authorization", $"Basic {authToken}");

        var res = await client.SendAsync(req);

        Console.WriteLine($"[PERMISSION] Status: {res.StatusCode}");
    }

}