using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using SenffTest.Messaging.Abstractions;
using SenffTest.Messaging.Abstractions.Options;

namespace SenffTest.Messaging.RabbitMQ;

public class RabbitMqPublisher(IRabbitMqConnection conn) : IMessagePublisher, IDisposable
{
    private readonly IRabbitMqConnection _conn = conn;
    private readonly ConcurrentDictionary<string, bool> _declaredQueues = new();
    private readonly object _disposeLock = new();

    private IConnection? _connection;
    private IModel? _channel;
    private bool _disposed;

    public async Task PublishAsync<T>(string queue, T message, int retryCount = 3)
    {
        await PublishAsync(queue, message, null!, retryCount); 
    }

    public async Task PublishAsync<T>(string queue, T message, PublishOptions options, int retryCount = 3)
    {
        if (_disposed) 
            throw new ObjectDisposedException(nameof(RabbitMqPublisher));

        Exception? lastException = null;

        for (int attempt = 1; attempt <= retryCount; attempt++)
        {
            try
            {
                Console.WriteLine($"[PUBLISH] Tentativa {attempt}/{retryCount}"); 

                await EnsureConnectedAsync();

                try
                {
                    _channel!.QueueDeclare(queue, 
                    durable: options?.QueueOptions?.Durable ?? true, 
                    exclusive: options?.QueueOptions?.Exclusive ?? false, 
                    autoDelete: options?.QueueOptions?.AutoDelete ?? false,
                    arguments: options?.QueueOptions?.Arguments);

                    Console.WriteLine($"[PUBLISH] QueueDeclare chamado");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[PUBLISH] QueueDeclare falhou: {ex.Message}");
                    throw; 
                }

                var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));
                
                var channel = GetChannel();
                
                var exchange = options?.Exchange ?? "";
                var routingKey = options?.RoutingKey ?? queue;
                
                var basicProperties = channel.CreateBasicProperties();
                basicProperties.Persistent = options?.Persistent ?? true;

                channel.BasicPublish(exchange, routingKey, basicProperties, body);

                Console.WriteLine($"[PUBLISH] Sucesso na tentativa {attempt}"); 
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;
                Console.WriteLine($"[PUBLISH] Falha tentativa {attempt}/{retryCount}: {ex.Message}");

                CleanupResources();

                if (attempt < retryCount)
                {
                    var delay = 300 * attempt;
                    Console.WriteLine($"[PUBLISH] Aguardando {delay}ms antes da próxima tentativa...");
                    await Task.Delay(delay);
                }
            }
        }

        throw new InvalidOperationException($"Falha ao publicar mensagem após {retryCount} tentativas", lastException);
    }

    private async Task EnsureConnectedAsync()
    {
        if (_connection?.IsOpen == true && _channel?.IsOpen == true)
            return;

        CleanupResources();

        _connection = _conn.CreateConnection();
        _channel = _connection.CreateModel();
    }

    private IModel GetChannel()
    {
        if (_channel == null || !_channel.IsOpen)
            throw new InvalidOperationException("Canal não disponível ou fechado");
        
        return _channel;
    }

    private void CleanupResources()
    {
        try
        {
            _channel?.Close();
            _channel?.Dispose();
        }
        catch { }
        
        try
        {
            _connection?.Close();
            _connection?.Dispose();
        }
        catch { }

        _channel = null;
        _connection = null;
    }

    public void Dispose()
    {
        lock (_disposeLock)
        {
            if (_disposed) return;

            try
            {
                _channel?.Close();
                _channel?.Dispose();
            }
            catch { }

            try
            {
                _connection?.Close();
                _connection?.Dispose();
            }
            catch { }

            _declaredQueues.Clear();
            _disposed = true;
            GC.SuppressFinalize(this);
        }
    }
}