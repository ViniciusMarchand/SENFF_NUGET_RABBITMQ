namespace SenffTest.Messaging.Abstractions;

public interface IMessagePublisher
{
    Task PublishAsync<T>(string queue, T message, int retryCount = 3);
}
