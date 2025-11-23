using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SenffTest.Messaging.RabbitMQ;
using System.Text;
using Xunit;

namespace SenffTest.Tests;

public class ConsumerTests
{
    private readonly Mock<IRabbitMqConnection> _connectionMock;
    private readonly Mock<IConnection> _mqConnectionMock;
    private readonly Mock<IModel> _modelMock;
    private readonly RabbitMqConsumer _consumer;
    private IBasicConsumer? _registeredConsumer;

    public ConsumerTests()
    {
        _connectionMock = new Mock<IRabbitMqConnection>();
        _mqConnectionMock = new Mock<IConnection>();
        _modelMock = new Mock<IModel>();

        _connectionMock
            .Setup(c => c.CreateConnection())
            .Returns(_mqConnectionMock.Object);

        _mqConnectionMock
            .Setup(c => c.CreateModel())
            .Returns(_modelMock.Object);

        SetupModelMocks();
        _consumer = new RabbitMqConsumer(_connectionMock.Object);
    }

    private void SetupModelMocks()
    {
        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(),
                   It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Returns(new QueueDeclareOk("test.queue", 0, 0));

        _modelMock
            .Setup(m => m.BasicConsume(It.IsAny<string>(), It.IsAny<bool>(),
                   It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(),
                   It.IsAny<IDictionary<string, object>>(), It.IsAny<IBasicConsumer>()))
            .Callback<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>(
                (queue, autoAck, consumerTag, noLocal, exclusive, arguments, consumer) =>
                {
                    _registeredConsumer = consumer;
                })
            .Returns("consumer-tag");

        _modelMock
            .Setup(m => m.CreateBasicProperties())
            .Returns(new Mock<IBasicProperties>().Object);
    }

    [Fact]
    public void Consumer_Deve_Iniciar_Sem_Erros()
    {
        // Act
        _consumer.Consume<string>("queue.test", msg => Task.CompletedTask);

        // Assert
        _mqConnectionMock.Verify(c => c.CreateModel(), Times.Once);
        _modelMock.Verify(m => m.QueueDeclare(
            "queue.test",
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object>>()),
            Times.Once);

        _modelMock.Verify(m => m.BasicConsume(
            "queue.test",
            It.IsAny<bool>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object>>(),
            It.IsAny<IBasicConsumer>()),
            Times.Once);
    }

    [Fact]
    public void Consumer_Deve_Processar_Mensagem_String_Corretamente()
    {
        // Arrange & Act
        _consumer.Consume<string>("queue.test", msg => Task.CompletedTask);

        // Assert - Verifica que o consumer foi registrado
        Assert.NotNull(_registeredConsumer);
    }

    [Fact]
    public void Consumer_Deve_Processar_Mensagem_JSON_Corretamente()
    {
        // Arrange & Act
        _consumer.Consume<TestMessage>("queue.test", msg => Task.CompletedTask);

        // Assert - Verifica que o consumer foi registrado
        Assert.NotNull(_registeredConsumer);
    }

    [Fact]
    public void Consumer_Deve_Declarar_Fila_Com_Parametros_Corretos()
    {
        // Act
        _consumer.Consume<string>("queue.test", msg => Task.CompletedTask);

        // Assert
        _modelMock.Verify(m => m.QueueDeclare(
            "queue.test",   // queue name
            true,           // durable
            false,          // exclusive
            false,          // autoDelete
            It.IsAny<IDictionary<string, object>>()),
            Times.Once);
    }

    [Fact]
    public void Consumer_Deve_Configurar_BasicConsume_Com_Parametros_Corretos()
    {
        // Act
        _consumer.Consume<string>("queue.test", msg => Task.CompletedTask);

        // Assert
        _modelMock.Verify(m => m.BasicConsume(
            "queue.test",   // queue
            false,          // autoAck
            It.IsAny<string>(), // consumerTag
            false,          // noLocal
            false,          // exclusive
            It.IsAny<IDictionary<string, object>>(), // arguments
            It.IsAny<IBasicConsumer>()), // consumer
            Times.Once);
    }

    [Fact]
    public void Consumer_Deve_Lancar_Excecao_Quando_Conexao_Falha()
    {
        // Arrange
        _connectionMock
            .Setup(c => c.CreateConnection())
            .Throws(new Exception("Connection failed"));

        var consumer = new RabbitMqConsumer(_connectionMock.Object);

        // Act & Assert
        Assert.Throws<Exception>(() =>
            consumer.Consume<string>("queue.test", msg => Task.CompletedTask));
    }

    [Fact]
    public void Consumer_Deve_Processar_Multiplas_Filas()
    {
        // Act
        _consumer.Consume<string>("queue1", msg => Task.CompletedTask);
        _consumer.Consume<string>("queue2", msg => Task.CompletedTask);

        // Assert
        _modelMock.Verify(m => m.QueueDeclare("queue1", It.IsAny<bool>(), It.IsAny<bool>(),
            It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()), Times.Once);
        _modelMock.Verify(m => m.QueueDeclare("queue2", It.IsAny<bool>(), It.IsAny<bool>(),
            It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()), Times.Once);
    }

    [Fact]
    public void Consumer_Deve_Criar_Connection_Uma_Vez()
    {
        // Act
        _consumer.Consume<string>("queue.test", msg => Task.CompletedTask);

        // Assert
        _connectionMock.Verify(c => c.CreateConnection(), Times.Once);
    }

    [Fact]
    public void Consumer_Deve_Criar_Model_Uma_Vez()
    {
        // Act
        _consumer.Consume<string>("queue.test", msg => Task.CompletedTask);

        // Assert
        _mqConnectionMock.Verify(c => c.CreateModel(), Times.Once);
    }

    [Fact]
    public void Consumer_Deve_Registrar_Consumer_Para_Fila()
    {
        // Act
        _consumer.Consume<string>("queue.test", msg => Task.CompletedTask);

        // Assert
        _modelMock.Verify(m => m.BasicConsume(
            "queue.test",
            It.IsAny<bool>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object>>(),
            It.IsAny<IBasicConsumer>()),
            Times.Once);
    }

    [Fact]
    public void Consumer_Deve_Declarar_Fila_Antes_De_Consumir()
    {
        // Arrange
        var invocations = new List<string>();

        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Callback<string, bool, bool, bool, IDictionary<string, object>>((q, d, e, a, args) => invocations.Add("QueueDeclare"))
            .Returns(new QueueDeclareOk("test.queue", 0, 0));

        _modelMock
            .Setup(m => m.BasicConsume(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<IBasicConsumer>()))
            .Callback<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>((q, aa, ct, nl, e, args, c) => invocations.Add("BasicConsume"))
            .Returns("consumer-tag");

        // Act
        _consumer.Consume<string>("queue.test", msg => Task.CompletedTask);

        // Assert - Verifica a ordem das chamadas
        Assert.Equal(2, invocations.Count);
        Assert.Equal("QueueDeclare", invocations[0]);
        Assert.Equal("BasicConsume", invocations[1]);
    }

    [Fact]
    public void Consumer_Deve_Usar_QueueName_Correta_Em_Todas_Chamadas()
    {
        // Arrange
        const string queueName = "minha.fila";
        var declaredQueues = new List<string>();
        var consumedQueues = new List<string>();

        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Callback<string, bool, bool, bool, IDictionary<string, object>>((q, d, e, a, args) => declaredQueues.Add(q))
            .Returns(new QueueDeclareOk(queueName, 0, 0));

        _modelMock
            .Setup(m => m.BasicConsume(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<IBasicConsumer>()))
            .Callback<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>((q, aa, ct, nl, e, args, c) => consumedQueues.Add(q))
            .Returns("consumer-tag");

        // Act
        _consumer.Consume<string>(queueName, msg => Task.CompletedTask);

        // Assert
        Assert.Single(declaredQueues);
        Assert.Single(consumedQueues);
        Assert.Equal(queueName, declaredQueues[0]);
        Assert.Equal(queueName, consumedQueues[0]);
    }

    [Fact]
    public async Task Consumer_Deve_Tentar_Novamente_Quando_Handler_Falhar()
    {
        // Arrange
        var attemptCount = 0;
        var maxAttempts = 3;
        var messageProcessed = false;
        var tcs = new TaskCompletionSource<bool>();

        var consumer = new AsyncEventingBasicConsumer(_modelMock.Object);

        _modelMock
            .Setup(m => m.BasicConsume(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<IBasicConsumer>()))
            .Callback<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>((queue, autoAck, consumerTag, noLocal, exclusive, arguments, basicConsumer) =>
            {
                consumer = (AsyncEventingBasicConsumer)basicConsumer;
            });

        // Act
        _consumer.Consume<string>("queue.test", async (msg) =>
        {
            attemptCount++;
            if (attemptCount < maxAttempts)
            {
                throw new Exception($"Falha simulada no handler - tentativa {attemptCount}");
            }
            messageProcessed = true;
            tcs.SetResult(true);
            await Task.CompletedTask;
        }, retryCount: 2);

        // Simula o recebimento de uma mensagem
        var messageBody = Encoding.UTF8.GetBytes("\"mensagem de teste\"");
        var eventArgs = new BasicDeliverEventArgs
        {
            Body = new ReadOnlyMemory<byte>(messageBody),
            DeliveryTag = 123UL
        };

        await consumer.HandleBasicDeliver(
            "consumer-tag",
            eventArgs.DeliveryTag,
            eventArgs.Redelivered,
            eventArgs.Exchange,
            eventArgs.RoutingKey,
            eventArgs.BasicProperties,
            eventArgs.Body);

        // Aguarda o processamento
        await tcs.Task;

        // Assert
        Assert.Equal(3, attemptCount);
        Assert.True(messageProcessed);
    }
}

public class TestMessage
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}