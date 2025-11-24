using WebApp.DTOs;

namespace WebApp.Services.Interfaces;

public interface INoteService
{
    Task SendNote(Note note);
    Task TestRetry(TestRetryRequest dto);
    Task TestRetryConsumer(TestRetryRequest dto);
}