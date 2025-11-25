using SenffTest.Messaging.Abstractions.Options;
using SenffTest.Messaging.RabbitMQ;
using WebApp.Services.Interfaces;

namespace WebApp.Services;

public class RabbitMQService : IRabbitMQService
{

    private readonly RabbitMqConnection _rabbitMqConnection;
    private readonly RabbitMqPublisher _publisher;
    private readonly RabbitMqConsumer _consumer;

    public RabbitMQService()
    {
        _rabbitMqConnection = new RabbitMqConnection("host.docker.internal", "guest", "guest");
        _publisher = new RabbitMqPublisher(_rabbitMqConnection);
        _consumer = new RabbitMqConsumer(_rabbitMqConnection);
    }

    public async Task SendMessage<T>(string queue, T message, int retryCount = 3)
    {
        await _publisher.PublishAsync(queue, message, retryCount);
    }

    public async Task SendMessage<T>(string queue, T message, PublishOptions options, int retryCount = 3)
    {
        await _publisher.PublishAsync(queue, message, options, retryCount);
    }

    public void StartConsumer<T>(string queue, Func<T, Task> onMessage, int retryCount = 3)
    {
        _consumer.Consume<T>(queue, async msg =>
        {
            await onMessage(msg);
        }, retryCount);
    }

    public void StartConsumer<T>(string queue, Func<T, Task> onMessage, QueueOptions options, int retryCount = 3)
    {
        _consumer.Consume<T>(queue, async msg =>
        {
            await onMessage(msg);
        }, options, retryCount);    
    }
}