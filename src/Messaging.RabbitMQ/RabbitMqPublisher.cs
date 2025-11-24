using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using SenffTest.Messaging.Abstractions;

namespace SenffTest.Messaging.RabbitMQ;

public class RabbitMqPublisher : IMessagePublisher, IDisposable
{
    private readonly IRabbitMqConnection _conn;

    private IConnection _connection;
    private IModel _channel;

    public RabbitMqPublisher(IRabbitMqConnection conn)
    {
        _conn = conn;

        _connection = _conn.CreateConnection();
        _channel = _connection.CreateModel();
    }

    public Task PublishAsync<T>(string queue, T message, int retryCount = 3)
    {
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        for (int attempt = 1; attempt <= retryCount; attempt++)
        {
            try
            {
                _channel.QueueDeclare(
                    queue: queue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );

                _channel.BasicPublish(
                    exchange: "",
                    routingKey: queue,
                    basicProperties: null,
                    body: body
                );

                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[PUBLISH] Falha tentativa {attempt}/{retryCount}: {ex.Message}");

                try
                {
                    _channel?.Dispose();
                    _connection?.Dispose();
                }
                catch { }

                _connection = _conn.CreateConnection();
                _channel = _connection.CreateModel();

                if (attempt == retryCount)
                    throw;

                Thread.Sleep(300 * attempt);
            }
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        try { _channel?.Close(); } catch { }
        try { _connection?.Close(); } catch { }
    }
}
