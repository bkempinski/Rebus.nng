using Microsoft.Extensions.Logging;
using Rebus.Bus;
using Rebus.Handlers;
using Rebus.nng.TestConsole.Messages;

namespace Rebus.nng.TestConsole.Handlers;

public class SimpleMsgHandler : IHandleMessages<SimpleMsg>
{
    private readonly ILogger _logger;
    private readonly IBus _bus;

    public SimpleMsgHandler
        (
            ILogger logger,
            IBus bus
        ) => (_logger, _bus) = (logger, bus);

    public Task Handle(SimpleMsg message)
    {
        _logger.LogInformation($"[{_bus}] Message recieved: {message.Message}");

        return Task.CompletedTask;
    }
}