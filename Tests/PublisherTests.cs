using Moq;
using RabbitMQ.Client;
using SenffTest.Messaging.RabbitMQ;
using System.Text;
using System.Text.Json;
using Xunit;

namespace SenffTest.Tests;

public class PublisherTests
{
    private readonly Mock<IRabbitMqConnection> _connectionMock;
    private readonly Mock<IConnection> _mqConnectionMock;
    private readonly Mock<IModel> _modelMock;
    private readonly RabbitMqPublisher _publisher;

    public PublisherTests()
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
        _publisher = new RabbitMqPublisher(_connectionMock.Object);
    }

    private void SetupModelMocks()
    {
        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(),
                   It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Returns(new QueueDeclareOk("test.queue", 0, 0));

        _modelMock
            .Setup(m => m.CreateBasicProperties())
            .Returns(new Mock<IBasicProperties>().Object);

        _modelMock
            .Setup(m => m.BasicPublish(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<IBasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>()));
    }

    [Fact]
    public async Task Publisher_Deve_Publicar_Sem_Erros()
    {
        // Act
        await _publisher.PublishAsync("queue.test", "mensagem");

        // Assert
        _mqConnectionMock.Verify(c => c.CreateModel(), Times.Once);
        _modelMock.Verify(m => m.BasicPublish(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
    }

    [Fact]
    public async Task Publisher_Deve_Declarar_Fila_Antes_De_Publicar()
    {
        // Arrange
        var invocations = new List<string>();

        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Callback<string, bool, bool, bool, IDictionary<string, object>>((q, d, e, a, args) => invocations.Add("QueueDeclare"))
            .Returns(new QueueDeclareOk("test.queue", 0, 0));

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>((e, rk, m, p, b) => invocations.Add("BasicPublish"));

        // Act
        await _publisher.PublishAsync("queue.test", "mensagem");

        // Assert - Verifica a ordem das chamadas
        Assert.Equal(2, invocations.Count);
        Assert.Equal("QueueDeclare", invocations[0]);
        Assert.Equal("BasicPublish", invocations[1]);
    }

    [Fact]
    public async Task Publisher_Deve_Publicar_Mensagem_String_Como_JSON()
    {
        // Arrange
        var expectedMessage = "Hello World";
        byte[]? actualMessageBody = null;

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>((exchange, routingKey, mandatory, properties, body) =>
            {
                actualMessageBody = body.ToArray();
            });

        // Act
        await _publisher.PublishAsync("queue.test", expectedMessage);

        // Assert - A mensagem deve ser serializada como JSON
        Assert.NotNull(actualMessageBody);
        var actualMessage = Encoding.UTF8.GetString(actualMessageBody);
        var expectedJson = JsonSerializer.Serialize(expectedMessage);
        Assert.Equal(expectedJson, actualMessage);
    }

    [Fact]
    public async Task Publisher_Deve_Publicar_Mensagem_Objeto_Como_JSON()
    {
        // Arrange
        var expectedMessage = new TestMessage { Id = 1, Name = "Test" };
        byte[]? actualMessageBody = null;

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>((exchange, routingKey, mandatory, properties, body) =>
            {
                actualMessageBody = body.ToArray();
            });

        // Act
        await _publisher.PublishAsync("queue.test", expectedMessage);

        // Assert
        Assert.NotNull(actualMessageBody);
        var actualMessageJson = Encoding.UTF8.GetString(actualMessageBody);
        var expectedJson = JsonSerializer.Serialize(expectedMessage);
        Assert.Equal(expectedJson, actualMessageJson);
    }

    [Fact]
    public async Task Publisher_Deve_Publicar_Com_Exchange_Vazio_E_RoutingKey_Como_QueueName()
    {
        // Arrange
        const string queueName = "minha.fila";
        string? actualExchange = null;
        string? actualRoutingKey = null;

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>((exchange, routingKey, mandatory, properties, body) =>
            {
                actualExchange = exchange;
                actualRoutingKey = routingKey;
            });

        // Act
        await _publisher.PublishAsync(queueName, "mensagem");

        // Assert
        Assert.Equal(string.Empty, actualExchange);
        Assert.Equal(queueName, actualRoutingKey);
    }

    [Fact]
    public async Task Publisher_Deve_Declarar_Fila_Com_Parametros_Corretos()
    {
        // Arrange
        const string queueName = "minha.fila";
        string? declaredQueueName = null;
        bool? durable = null;
        bool? exclusive = null;
        bool? autoDelete = null;

        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Callback<string, bool, bool, bool, IDictionary<string, object>>((q, d, e, a, args) =>
            {
                declaredQueueName = q;
                durable = d;
                exclusive = e;
                autoDelete = a;
            })
            .Returns(new QueueDeclareOk(queueName, 0, 0));

        // Act
        await _publisher.PublishAsync(queueName, "mensagem");

        // Assert
        Assert.Equal(queueName, declaredQueueName);
        Assert.True(durable);
        Assert.False(exclusive);
        Assert.False(autoDelete);
    }

    [Fact]
    public async Task Publisher_Deve_Lancar_Excecao_Quando_Conexao_Falha()
    {
        // Arrange
        _connectionMock
            .Setup(c => c.CreateConnection())
            .Throws(new Exception("Connection failed"));

        var publisher = new RabbitMqPublisher(_connectionMock.Object);

        // Act & Assert
        await Assert.ThrowsAsync<Exception>(() =>
            publisher.PublishAsync("queue.test", "mensagem"));
    }

    [Fact]
    public async Task Publisher_Deve_Publicar_Para_Filas_Diferentes()
    {
        // Arrange
        var declaredQueues = new List<string>();
        var publishedQueues = new List<string>();

        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Callback<string, bool, bool, bool, IDictionary<string, object>>((q, d, e, a, args) => declaredQueues.Add(q))
            .Returns(new QueueDeclareOk("test.queue", 0, 0));

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>((exchange, routingKey, mandatory, properties, body) =>
            {
                publishedQueues.Add(routingKey);
            });

        // Act
        await _publisher.PublishAsync("fila1", "mensagem1");
        await _publisher.PublishAsync("fila2", "mensagem2");
        await _publisher.PublishAsync("fila3", "mensagem3");

        // Assert
        Assert.Equal(3, declaredQueues.Count);
        Assert.Equal(3, publishedQueues.Count);
        Assert.Contains("fila1", declaredQueues);
        Assert.Contains("fila2", declaredQueues);
        Assert.Contains("fila3", declaredQueues);
        Assert.Contains("fila1", publishedQueues);
        Assert.Contains("fila2", publishedQueues);
        Assert.Contains("fila3", publishedQueues);
    }

    [Fact]
    public async Task Publisher_Deve_Usar_QueueName_Correta_Em_Todas_Chamadas()
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
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>((exchange, routingKey, mandatory, properties, body) =>
            {
                consumedQueues.Add(routingKey);
            });

        // Act
        await _publisher.PublishAsync(queueName, "mensagem");

        // Assert
        Assert.Single(declaredQueues);
        Assert.Single(consumedQueues);
        Assert.Equal(queueName, declaredQueues[0]);
        Assert.Equal(queueName, consumedQueues[0]);
    }

    // Testes focados no comportamento real - SEM verificações de reutilização

    [Fact]
    public async Task Publisher_Deve_Chamar_BasicPublish_Uma_Vez_Por_Mensagem()
    {
        // Arrange
        var publishCount = 0;

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>((e, rk, m, p, b) =>
            {
                publishCount++;
            });

        // Act
        await _publisher.PublishAsync("queue.test", "mensagem1");
        await _publisher.PublishAsync("queue.test", "mensagem2");

        // Assert
        Assert.Equal(2, publishCount);
    }

    [Fact]
    public async Task Publisher_Deve_Serializar_Diferentes_Tipos_Corretamente()
    {
        // Arrange
        var publishedBodies = new List<string>();

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>((e, rk, m, p, b) =>
            {
                publishedBodies.Add(Encoding.UTF8.GetString(b.ToArray()));
            });

        // Act
        await _publisher.PublishAsync("queue.test", "string simples");
        await _publisher.PublishAsync("queue.test", 123);
        await _publisher.PublishAsync("queue.test", true);
        await _publisher.PublishAsync("queue.test", new { Propriedade = "valor" });

        // Assert
        Assert.Equal(4, publishedBodies.Count);
        Assert.Contains("string simples", publishedBodies[0]);
        Assert.Contains("123", publishedBodies[1]);
        Assert.Contains("true", publishedBodies[2]);
        Assert.Contains("Propriedade", publishedBodies[3]);
    }

    [Fact]
    public async Task Publisher_Deve_Tentar_Novamente_Quando_Publish_Falhar()
    {
        // Arrange
        var attemptCount = 0;
        var maxAttempts = 3;

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback(() =>
            {
                attemptCount++;
                if (attemptCount < maxAttempts)
                {
                    throw new Exception($"Falha simulada no publish - tentativa {attemptCount}");
                }
            });

        // Act
        await _publisher.PublishAsync("queue.test", "mensagem", retryCount: 2);

        // Assert - Deve ter tentado 3 vezes (2 retries + 1 original)
        Assert.Equal(3, attemptCount);
    }
}
