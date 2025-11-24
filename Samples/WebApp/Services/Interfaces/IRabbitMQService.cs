namespace WebApp.Services.Interfaces;

public interface IRabbitMQService
{
    Task SendMessage<T>(string queue, T message, int retryCount = 3);
    void StartConsumer<T>(string queue, Func<T, Task> onMessage, int retryCount = 3);
}
