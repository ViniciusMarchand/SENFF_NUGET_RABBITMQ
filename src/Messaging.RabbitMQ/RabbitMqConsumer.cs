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
        _ = Task.Run(async () =>
        {
            while (true)
            {
                IConnection? connection = null;
                IModel? channel = null;

                try
                {
                    connection = _conn.CreateConnection();
                    channel = connection.CreateModel();

                    channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);

                    var consumer = new AsyncEventingBasicConsumer(channel);

                    consumer.Received += async (_, args) =>
                    {
                        T? message = JsonSerializer.Deserialize<T>(args.Body.ToArray());

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

                    channel.BasicConsume(queue, autoAck: false, consumer);


                    while (connection.IsOpen && channel.IsOpen)
                    {
                        await Task.Delay(500);
                    }

                    Console.WriteLine("[CONSUMER] Canal ou conexão fechados. Tentando reconectar...");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CONSUMER] Falha ao conectar/consumir: {ex.Message}");
                }
                finally
                {
                    try { channel?.Close(); } catch {}
                    try { connection?.Close(); } catch {}
                }

                await Task.Delay(2000);
            }
        });
    }
}
