using SenffTest.Messaging.Abstractions;
namespace SenffTest.SampleApp.App;

public class MessagingTestService(IMessagePublisher publisher, IMessageConsumer consumer)
{
    private readonly IMessagePublisher _publisher = publisher;
    private readonly IMessageConsumer _consumer = consumer;

    public void StartConsumer(string queue)
    {
        _consumer.Consume<object>(queue, async msg =>
        {
            Console.WriteLine("[CONSUMER] Recebido: " + msg);
            await Task.CompletedTask;
        });
    }

    public async Task SendMessage(string queue, string message)
    {
        await _publisher.PublishAsync(queue, message);
    }
}
