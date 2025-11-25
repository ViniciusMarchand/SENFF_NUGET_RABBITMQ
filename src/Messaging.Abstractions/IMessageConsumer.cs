using SenffTest.Messaging.Abstractions.Options;

namespace SenffTest.Messaging.Abstractions;

public interface IMessageConsumer
{
    void Consume<T>(string queue, Func<T, Task> handler, int retryCount = 3);
    void Consume<T>(string queue, Func<T, Task> handler, QueueOptions options, int retryCount = 3);

}