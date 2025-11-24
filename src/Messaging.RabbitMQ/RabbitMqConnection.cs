using RabbitMQ.Client;

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
        int attempts = 0;

        while (true)
        {
            try
            {
                return _factory.CreateConnection();
            }
            catch (Exception ex)
            {
                attempts++;
                Console.WriteLine($"Falha ao conectar: {ex.Message}");

                if (attempts >= 20)
                    throw;

                Thread.Sleep(2000);
            }
        }
    }
}
