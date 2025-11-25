using SenffTest.Messaging.Abstractions.Options;

namespace SenffTest.Messaging.Abstractions;

public interface IMessagePublisher
{
    Task PublishAsync<T>(string queue, T message, int retryCount = 3);
    Task PublishAsync<T>(string queue, T message, PublishOptions options, int retryCount = 3);
}
