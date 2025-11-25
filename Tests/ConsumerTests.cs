using Moq;
using RabbitMQ.Client;
using SenffTest.Messaging.Abstractions.Options;
using SenffTest.Messaging.RabbitMQ;

namespace SenffTest.Tests;

public class RabbitMqConsumerTests
{
    private readonly Mock<IRabbitMqConnection> _connectionMock;
    private readonly Mock<IConnection> _mqConnectionMock;
    private readonly Mock<IModel> _modelMock;
    private readonly RabbitMqConsumer _consumer;

    public RabbitMqConsumerTests()
    {
        _connectionMock = new Mock<IRabbitMqConnection>();
        _mqConnectionMock = new Mock<IConnection>();
        _modelMock = new Mock<IModel>();

        _connectionMock.Setup(c => c.CreateConnection()).Returns(_mqConnectionMock.Object);
        _mqConnectionMock.Setup(c => c.CreateModel()).Returns(_modelMock.Object);

        _modelMock.Setup(m => m.IsOpen).Returns(true);
        _mqConnectionMock.Setup(c => c.IsOpen).Returns(true);

        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(),
                    It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Returns(new QueueDeclareOk("test.queue", 0, 0));

        _consumer = new RabbitMqConsumer(_connectionMock.Object);
    }

    [Fact]
    public void Consume_Deve_Configurar_Fila_E_Consumer_Corretamente()
    {
        var queueName = "test.queue";
        
        _consumer.Consume<string>(queueName, msg => Task.CompletedTask);

        Thread.Sleep(100);

        _modelMock.Verify(m => m.QueueDeclare(
            queueName,
            true,   // durable
            false,  // exclusive
            false,  // autoDelete
            It.IsAny<IDictionary<string, object>>()),
            Times.Once);

        _modelMock.Verify(m => m.BasicConsume(
            queueName,
            false,  // autoAck = false
            It.IsAny<string>(),
            false,  // noLocal
            false,  // exclusive
            It.IsAny<IDictionary<string, object>>(),
            It.IsAny<IBasicConsumer>()),
            Times.Once);
    }

    [Fact]
    public void Consume_Com_QueueOptions_Deve_Declarar_Fila_Com_Parametros_Corretos()
    {
        var queueName = "test-queue";
        var options = new QueueOptions
        {
            Durable = false,
            Exclusive = true,
            AutoDelete = true,
            Arguments = new Dictionary<string, object> { ["x-max-priority"] = 5 }
        };

        bool? declaredDurable = null;
        bool? declaredExclusive = null;
        bool? declaredAutoDelete = null;
        IDictionary<string, object>? declaredArgs = null;

        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(),
                    It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Callback<string, bool, bool, bool, IDictionary<string, object>>(
                (queue, durable, exclusive, autoDelete, args) =>
                {
                    declaredDurable = durable;
                    declaredExclusive = exclusive;
                    declaredAutoDelete = autoDelete;
                    declaredArgs = args;
                });

        _consumer.Consume<string>(queueName, msg => Task.CompletedTask, options);

        Thread.Sleep(100);

        Assert.NotNull(declaredDurable);
        Assert.False(declaredDurable); 
        Assert.True(declaredExclusive);   
        Assert.True(declaredAutoDelete);
        Assert.NotNull(declaredArgs);
        Assert.Contains("x-max-priority", declaredArgs.Keys);
        Assert.Equal(5, declaredArgs["x-max-priority"]);
    }

    [Fact]
    public void Consume_Com_QueueOptions_Null_Deve_Usar_Valores_Padrao()
    {
        var queueName = "test-queue";

        bool? declaredDurable = null;
        bool? declaredExclusive = null;
        bool? declaredAutoDelete = null;

        _modelMock
            .Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(),
                    It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
            .Callback<string, bool, bool, bool, IDictionary<string, object>>(
                (queue, durable, exclusive, autoDelete, args) =>
                {
                    declaredDurable = durable;
                    declaredExclusive = exclusive;
                    declaredAutoDelete = autoDelete;
                });

        _consumer.Consume<string>(queueName, msg => Task.CompletedTask);

        Thread.Sleep(100);

        Assert.NotNull(declaredDurable);
        Assert.True(declaredDurable);     
        Assert.False(declaredExclusive);  
        Assert.False(declaredAutoDelete); 
    }

    [Fact]
    public void Consume_Deve_Tentar_Criar_Conexao_Ao_Iniciar()
    {
        var queueName = "test.queue";

        _consumer.Consume<string>(queueName, msg => Task.CompletedTask);

        Thread.Sleep(100);

        _connectionMock.Verify(c => c.CreateConnection(), Times.Once);
        _mqConnectionMock.Verify(c => c.CreateModel(), Times.Once);
    }

}
