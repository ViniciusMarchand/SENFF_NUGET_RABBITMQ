namespace WebApp.Services.Interfaces;

public interface IRabbitMQService
{
    Task SendMessage<T>(string queue, T message);
    void StartConsumer<T>(string queue, Func<T, Task> onMessage);
}