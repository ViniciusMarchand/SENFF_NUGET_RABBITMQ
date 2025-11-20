using RabbitMQ.Client;

namespace Senff_test.Messaging.RabbitMQ;

public interface IRabbitMqConnection
{
    IConnection CreateConnection();
}
