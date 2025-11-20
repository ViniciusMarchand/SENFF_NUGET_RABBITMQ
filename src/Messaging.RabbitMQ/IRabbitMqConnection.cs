using RabbitMQ.Client;

namespace SenffTest.Messaging.RabbitMQ;

public interface IRabbitMqConnection
{
    IConnection CreateConnection();
}
