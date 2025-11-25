using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace SenffTest.Messaging.RabbitMQ;

public class RabbitMqConnection : IRabbitMqConnection
{
    private readonly ConnectionFactory _factory;

    public RabbitMqConnection(string host, string user, string pass)
    {
        _factory = new ConnectionFactory
        {
            HostName = host,
            UserName = user, 
            Password = pass,
            DispatchConsumersAsync = true
        };
    }

    public IConnection CreateConnection()
    {
        for (int attempt = 1; attempt <= 20; attempt++)
        {
            try
            {
                return _factory.CreateConnection();
            }
            catch (Exception ex) when (IsNetworkError(ex) && attempt < 20)
            {
                Console.WriteLine($"Falha ao conectar: {ex.Message}");
                Task.Delay(2000).Wait(); 
            }
        }

        throw new InvalidOperationException("Falha ao conectar após 20 tentativas");
    }

    private static bool IsNetworkError(Exception ex)
    {
        return ex is BrokerUnreachableException || 
               ex is TimeoutException;
    }
}