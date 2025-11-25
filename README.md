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

# 🧱 Exemplo completo (Samples/WebApp)

O repositório também inclui um **projeto WebAPI completo** demonstrando:

* uso em DI
* serviços reais
* publicações via endpoints REST

📍 O WebApp pode ser acessado em:

## 👉 [http://localhost:5117/swagger/index.html](http://localhost:5117/swagger/index.html)

Este exemplo é totalmente funcional e pode ser usado como referência de integração real, nele existe um único endpoint que printa no console a mensagem enviada através da utilização do RabbitMQ.

Esse aplicativo web possui 3 endpoints para teste:

/message (envia e recebe mensagens mostrando o resultado no terminal)
/message/test-retry-publisher (cancela permissões e tenta enviar mensagem até a permissão ser liberada de novo ou até alcançar o números de tentativas passadas pelo body)
/message/test-retry-consumer (memsa coisa que o de cima mas para o consumer)




---

# 📄 Licença

MIT
