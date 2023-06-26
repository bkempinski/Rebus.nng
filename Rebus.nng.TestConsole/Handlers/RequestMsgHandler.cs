using Microsoft.Extensions.Logging;
using Rebus.Bus;
using Rebus.Handlers;
using Rebus.nng.TestConsole.Messages;

namespace Rebus.nng.TestConsole.Handlers;

public class RequestMsgHandler : IHandleMessages<RequestMsg>
{
    private readonly ILogger _logger;
    private readonly IBus _bus;

    public RequestMsgHandler
        (
            ILogger logger,
            IBus bus
        ) => (_logger, _bus) = (logger, bus);

    public async Task Handle(RequestMsg message)
    {
        _logger.LogInformation($"[{_bus}] Request recieved: {message.Request}");

        await _bus.Reply(new ReplyMsg
        {
            Reply = $"{message.Request} World!"
        });
    }
}