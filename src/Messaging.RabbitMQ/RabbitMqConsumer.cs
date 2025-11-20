using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SenffTest.Messaging.Abstractions;

namespace SenffTest.Messaging.RabbitMQ;

public class RabbitMqConsumer(IRabbitMqConnection conn) : IMessageConsumer
{
    private readonly IRabbitMqConnection _conn = conn;

    public void Consume<T>(string queue, Func<T, Task> handler, int retryCount = 3)
    {
        var connection = _conn.CreateConnection();
        var channel = connection.CreateModel();

        channel.QueueDeclare(queue, true, false, false);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.Received += async (sender, args) =>
        {
            string json = Encoding.UTF8.GetString(args.Body.ToArray());
            T? message = JsonSerializer.Deserialize<T>(json);

            int attempts = 0;

            while (true)
            {
                try
                {
                    await handler(message!);
                    channel.BasicAck(args.DeliveryTag, false);
                    break;
                }
                catch
                {
                    attempts++;

                    if (attempts > retryCount)
                    {
                        channel.BasicNack(args.DeliveryTag, false, false);
                        break;
                    }

                    await Task.Delay(500 * attempts);
                }
            }
        };

        channel.BasicConsume(queue, false, consumer);
    }
}
