using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using SenffTest.Messaging.Abstractions;
using SenffTest.Messaging.Abstractions.Options;

namespace SenffTest.Messaging.RabbitMQ;

public class RabbitMqConsumer(IRabbitMqConnection conn) : IMessageConsumer, IDisposable
{
    private readonly IRabbitMqConnection _conn = conn;
    private readonly CancellationTokenSource _cts = new();
    private bool _disposed;

    public void Consume<T>(string queue, Func<T, Task> handler, int retryCount = 3)
    {
        Consume(queue, handler, null!, retryCount);
    }

    public void Consume<T>(string queue, Func<T, Task> handler, QueueOptions options, int retryCount = 3)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RabbitMqConsumer));

        _ = Task.Run(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                IConnection? connection = null;
                IModel? channel = null;
                string? consumerTag = null;

                try
                {
                    connection = _conn.CreateConnection();
                    channel = connection.CreateModel();

                    var queueOptions = options ?? new QueueOptions();
                    channel.QueueDeclare(
                        queue: queue,
                        durable: queueOptions.Durable,
                        exclusive: queueOptions.Exclusive,
                        autoDelete: queueOptions.AutoDelete,
                        arguments: queueOptions.Arguments
                    );

                    var consumer = new AsyncEventingBasicConsumer(channel);

                    consumer.Received += async (_, args) =>
                    {
                        try
                        {
                            var message = JsonSerializer.Deserialize<T>(args.Body.Span);
                            if (message == null)
                            {
                                channel.BasicNack(args.DeliveryTag, false, false);
                                return;
                            }

                            int attempts = 0;

                            while (true)
                            {
                                try
                                {
                                    await handler(message);
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
                        }
                        catch (Exception ex) when (IsChannelClosed(ex))
                        {
                            Console.WriteLine($"[CONSUMER] Canal fechado durante processamento: {ex.Message}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[CONSUMER] Erro inesperado: {ex.Message}");
                            try { channel.BasicNack(args.DeliveryTag, false, false); } catch { }
                        }
                    };

                    consumerTag = channel.BasicConsume(queue, autoAck: false, consumer);

                    while (connection.IsOpen && channel.IsOpen && !_cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            channel.QueueDeclarePassive(queue);
                            await Task.Delay(500, _cts.Token);
                        }
                        catch (OperationInterruptedException ex) when (ex.Message.Contains("NOT_FOUND"))
                        {
                            Console.WriteLine($"[CONSUMER] Fila '{queue}' foi apagada! Reconectando...");
                            break; 
                        }
                    }

                    if (!_cts.Token.IsCancellationRequested)
                    {
                        Console.WriteLine("[CONSUMER] Canal ou conexão fechados. Tentando reconectar...");
                    }
                }
                catch (Exception ex) when (!_cts.Token.IsCancellationRequested)
                {
                    Console.WriteLine($"[CONSUMER] Falha ao conectar/consumir: {ex.Message}");
                }
                finally
                {
                    try
                    {
                        if (consumerTag != null && channel?.IsOpen == true)
                            channel.BasicCancel(consumerTag);
                    }
                    catch { }

                    try { channel?.Close(); } catch { }
                    try { connection?.Close(); } catch { }
                }

                if (!_cts.Token.IsCancellationRequested)
                    await Task.Delay(2000, _cts.Token);
            }
        }, _cts.Token);
    }

    private static bool IsChannelClosed(Exception ex)
    {
        return ex is OperationInterruptedException ||
               ex.Message.Contains("channel") && ex.Message.Contains("closed");
    }

    public void Dispose()
    {
        if (_disposed) return;

        _cts.Cancel();
        _cts.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
    }
}