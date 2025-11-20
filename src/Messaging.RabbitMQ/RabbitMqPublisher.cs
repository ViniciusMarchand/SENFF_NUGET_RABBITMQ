using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using SenffTest.Messaging.Abstractions;

namespace SenffTest.Messaging.RabbitMQ;

public class RabbitMqPublisher(IRabbitMqConnection conn) : IMessagePublisher
{
    private readonly IRabbitMqConnection _conn = conn;

    public async Task PublishAsync<T>(string queue, T message, int retryCount = 3)
    {
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        for (int attempt = 0; attempt <= retryCount; attempt++)
        {
            try
            {
                using var connection = _conn.CreateConnection();
                using var channel = connection.CreateModel();

                channel.QueueDeclare(queue, true, false, false);

                channel.BasicPublish(
                    exchange: "",
                    routingKey: queue,
                    basicProperties: null,
                    body: body
                );

                return;
            }
            catch when (attempt < retryCount)
            {
                await Task.Delay(500 * (attempt + 1)); 
            }
        }

        throw new Exception($"Failed to publish message after {retryCount} retries.");
    }
}
