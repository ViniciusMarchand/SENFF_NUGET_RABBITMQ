using SenffTest.Messaging.Abstractions;
using SenffTest.Messaging.RabbitMQ;
using SenffTest.SampleApp.App;

Console.WriteLine("Iniciando TestApp...");

var connection = new RabbitMqConnection(
    "localhost",
    "guest",
    "guest"
);

var publisher = new RabbitMqPublisher(connection);
var consumer = new RabbitMqConsumer(connection);

var service = new MessagingTestService(publisher, consumer);

string fila = "queue";

service.StartConsumer(fila);

Console.Write("Digite uma mensagem: ");
string? texto = Console.ReadLine();

await service.SendMessage(fila, texto ?? "mensagem vazia");

Console.WriteLine("Mensagem enviada!");
Console.ReadKey();
