using System.ComponentModel;
using System.Text.Json.Serialization;

namespace WebApp.DTOs;

public record TestRetryRequest(Note Note)
{
    [DefaultValue(5000)]
    public int TimeWithErrorMs { get; set; }

    [DefaultValue(50)]
    public int RetryQuantity { get; set; }
}