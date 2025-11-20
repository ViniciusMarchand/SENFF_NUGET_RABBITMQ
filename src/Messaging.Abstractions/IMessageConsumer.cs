namespace SenffTest.Messaging.Abstractions;

public interface IMessageConsumer
{
    void Consume<T>(string queue, Func<T, Task> handler, int retryCount = 3);
}