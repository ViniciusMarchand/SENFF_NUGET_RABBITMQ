using Moq;
using RabbitMQ.Client;
using SenffTest.Messaging.Abstractions.Options;
using SenffTest.Messaging.RabbitMQ;


namespace SenffTest.Tests;

public class RabbitMqPublisherTests
{
    private readonly Mock<IRabbitMqConnection> _connectionMock;
    private readonly Mock<IConnection> _mqConnectionMock;
    private readonly Mock<IModel> _modelMock;
    private readonly Mock<IBasicProperties> _basicPropertiesMock;
    private readonly RabbitMqPublisher _publisher;

    public RabbitMqPublisherTests()
    {
        _connectionMock = new Mock<IRabbitMqConnection>();
        _mqConnectionMock = new Mock<IConnection>();
        _modelMock = new Mock<IModel>();
        _basicPropertiesMock = new Mock<IBasicProperties>();

        _connectionMock.Setup(c => c.CreateConnection()).Returns(_mqConnectionMock.Object);
        _mqConnectionMock.Setup(c => c.CreateModel()).Returns(_modelMock.Object);

        _modelMock.Setup(m => m.CreateBasicProperties()).Returns(_basicPropertiesMock.Object);

        _basicPropertiesMock.SetupAllProperties();
        _basicPropertiesMock.Object.Persistent = true; 

        _modelMock.Setup(m => m.IsOpen).Returns(true);
        _mqConnectionMock.Setup(c => c.IsOpen).Returns(true);

        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(),
                    It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Returns(new QueueDeclareOk("test.queue", 0, 0));

        _publisher = new RabbitMqPublisher(_connectionMock.Object);
    }

    [Fact]
    public async Task PublishAsync_Deve_Declarar_Fila_Com_Parametros_Corretos()
    {
        var queueName = "test.queue";
        var message = "Hello World";

        await _publisher.PublishAsync(queueName, message);

        _modelMock.Verify(m => m.QueueDeclare(
            queueName,
            true,   
            false,  
            false, 
            It.IsAny<IDictionary<string, object>>()), 
            Times.Once);
    }

    [Fact]
    public async Task PublishAsync_Com_PublishOptions_Deve_Usar_Exchange_E_RoutingKey_Corretos()
    {
        var queueName = "test.queue";
        var message = "test message";
        var options = new PublishOptions
        {
            Exchange = "custom-exchange",
            RoutingKey = "custom.routing.key",
            Persistent = false
        };

        string? usedExchange = null;
        string? usedRoutingKey = null;
        bool? usedPersistent = null;

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                    It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>(
                (exchange, routingKey, mandatory, properties, body) =>
                {
                    usedExchange = exchange;
                    usedRoutingKey = routingKey;
                    usedPersistent = properties.Persistent;
                });

        await _publisher.PublishAsync(queueName, message, options);

        Assert.Equal("custom-exchange", usedExchange);
        Assert.Equal("custom.routing.key", usedRoutingKey);
        Assert.False(usedPersistent);
    }

    [Fact]
    public async Task PublishAsync_Com_PublishOptions_Null_Deve_Usar_Valores_Padrao()
    {
        var queueName = "test.queue";
        var message = "test message";

        string? usedExchange = null;
        string? usedRoutingKey = null;
        bool? usedPersistent = null;

        _modelMock
            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                    It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>(
                (exchange, routingKey, mandatory, properties, body) =>
                {
                    usedExchange = exchange;
                    usedRoutingKey = routingKey;
                    usedPersistent = properties.Persistent;
                });

        await _publisher.PublishAsync(queueName, message);

        Assert.Equal("", usedExchange); 
        Assert.Equal(queueName, usedRoutingKey); 
        Assert.True(usedPersistent); 
    }
}

public class TestMessage
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}