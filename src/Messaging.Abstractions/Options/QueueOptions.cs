namespace SenffTest.Messaging.Abstractions.Options;

public class QueueOptions
{
    public bool Durable { get; set; } = true;
    public bool Exclusive { get; set; } = false;
    public bool AutoDelete { get; set; } = false;
    public IDictionary<string, object>? Arguments { get; set; }
}
