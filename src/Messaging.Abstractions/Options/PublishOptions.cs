namespace SenffTest.Messaging.Abstractions.Options;

public class PublishOptions
{
    public string? Exchange { get; set; }
    public string? RoutingKey { get; set; }
    public bool Persistent { get; set; } = true;
    public QueueOptions? QueueOptions { get; set; }
}