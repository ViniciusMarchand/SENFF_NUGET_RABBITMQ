# **SenffTest.Messaging (RabbitMQ)**

Biblioteca de mensageria em .NET baseada em **RabbitMQ**, oferecendo uma implementação simples, resiliente e desacoplada de Publisher/Consumer.

A solução contém:

* **SenffTest.Messaging.RabbitMQ.Abstraction** → Interfaces
* **SenffTest.Messaging.RabbitMQ** → Implementação RabbitMQ
* **Samples/App** → Exemplo console mínimo
* **Samples/WebApp** → Exemplo completo de uso real (API Web)
* **Tests** → Testes unitários

---

# 🚀 Como rodar o projeto (Docker)

O repositório já inclui um `docker-compose.yml` configurado para subir:

* **RabbitMQ (com painel de gerenciamento)**
* **WebApp de demonstração (Samples/WebApp)**

### 📌 Para iniciar tudo:

```
docker compose up
```

Isso iniciará dois serviços:

| Serviço                   | Endereço                                                                             |
| ------------------------- | ------------------------------------------------------------------------------------ |
| WebApp de teste (Swagger) | [http://localhost:5117/swagger/index.html](http://localhost:5117/swagger/index.html) |
| RabbitMQ Management       | [http://localhost:15672/#/](http://localhost:15672/#/)                               |

Credenciais do painel RabbitMQ:

* **Usuário:** guest
* **Senha:** guest

---

# 📦 Instalação via NuGet

Instale apenas o pacote principal:

```
dotnet add package SenffTest.Messaging.RabbitMQ
```

O pacote já inclui automaticamente:

* `SenffTest.Messaging.RabbitMQ.Abstraction`
* `RabbitMQ.Client`

---

# 🧩 Como usar (exemplo mínimo — Samples/App)

```csharp
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
```

---

# RabbitMqPublisher (Interface) 
```csharp
public interface IMessagePublisher
{
    Task PublishAsync<T>(string queue, T message, int retryCount = 3);
    Task PublishAsync<T>(string queue, T message, PublishOptions options, int retryCount = 3);
}
```
# RabbitMqConsumer (Interface) 
```csharp
public interface IMessageConsumer
{
    void Consume<T>(string queue, Func<T, Task> handler, int retryCount = 3);
    void Consume<T>(string queue, Func<T, Task> handler, QueueOptions options, int retryCount = 3);
}
```
# 🧱 Exemplo completo (Samples/WebApp)

O repositório também inclui um **projeto WebAPI completo** demonstrando:

* uso em DI
* serviços reais
* publicações via endpoints REST

📍 O WebApp pode ser acessado em:

## 👉 [http://localhost:5117/swagger/index.html](http://localhost:5117/swagger/index.html)

Este exemplo é totalmente funcional e pode ser usado como referência de integração real, nele existe um único endpoint que printa no console a mensagem enviada através da utilização do RabbitMQ.

# 🧪 Endpoints de Teste

| Endpoint | Descrição |
|----------|-----------|
| **`POST /message`** | Envia e recebe mensagens mostrando o resultado no terminal |
| **`POST /message/test-retry-publisher`** | Testa retry no publisher até liberar permissões ou atingir tentativas máximas |
| **`POST /message/test-retry-consumer`** | Testa retry no consumer até liberar permissões ou atingir tentativas máximas |



---

# 📄 Licença

MIT
