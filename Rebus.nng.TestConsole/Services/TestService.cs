using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rebus.Bus;
using Rebus.Config;
using Rebus.nng.TestConsole.Messages;
using Rebus.nng.Transport;
using Rebus.Routing.TypeBased;

namespace Rebus.nng.TestConsole.Services;

public class TestService : BackgroundService
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<TestService> _logger;

    public TestService(ILoggerFactory loggerFactory)
        => (_loggerFactory, _logger) = (loggerFactory, loggerFactory.CreateLogger<TestService>());

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.WhenAll
            (
                ProdConsTest(stoppingToken),
                RepReqTest(stoppingToken),
                PubSubTest(stoppingToken)
            );
    }

    private async Task ProdConsTest(CancellationToken stoppingToken)
    {
        var producer = GetProducerServices()
            .GetRequiredService<IBus>();
        var consumer1 = GetConsumerServices(1)
            .GetRequiredService<IBus>();
        var consumer2 = GetConsumerServices(2)
            .GetRequiredService<IBus>();

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(2000, stoppingToken);

            await producer.Publish(new SimpleMsg
            {
                Message = "Hello World!"
            });
        }
    }

    private async Task RepReqTest(CancellationToken stoppingToken)
    {
        var reply = GetReplyServices()
            .GetRequiredService<IBus>();
        var request1 = GetRequestServices(1)
            .GetRequiredService<IBus>();
        var request2 = GetRequestServices(2)
            .GetRequiredService<IBus>();

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(2000, stoppingToken);

            var replyMsg1 = await request1.SendRequest<ReplyMsg>(new RequestMsg
            {
                Request = "Hello 1"
            }, timeout: TimeSpan.FromSeconds(5));

            _logger.LogInformation($"[Request1] {replyMsg1}");

            var replyMsg2 = await request2.SendRequest<ReplyMsg>(new RequestMsg
            {
                Request = "Hello 2"
            }, timeout: TimeSpan.FromSeconds(5));

            _logger.LogInformation($"[request2] {replyMsg2}");
        }
    }

    private async Task PubSubTest(CancellationToken stoppingToken)
    {
        var publisher = GetPublisherServices()
            .GetRequiredService<IBus>();
        var subscriber1 = GetSubscriberServices(1)
            .GetRequiredService<IBus>();
        var subscriber2 = GetSubscriberServices(2)
            .GetRequiredService<IBus>();

        await subscriber1.Subscribe<SimpleMsg>();
        await subscriber2.Advanced.Topics.Subscribe("test_topic");

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(2000, stoppingToken);

            await publisher.Publish(new SimpleMsg
            {
                Message = "Hello Type-based Subscriber!"
            });

            await publisher.Advanced.Topics.Publish("test_topic", new SimpleMsg
            {
                Message = "Hello Topic Subscriber!"
            });
        }
    }

    private IServiceProvider GetProducerServices()
    {
        var services = new ServiceCollection();

        services
            .AddSingleton(_loggerFactory.CreateLogger("Producer"))
            .AddRebus((configure, services) => configure
                .Logging(l => l.MicrosoftExtensionsLogging(_loggerFactory))
                .Options(o =>
                {
                    o.SetBusName("Producer");
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);
                })
                .Transport(t => t.UseNngProducer("inproc://prodcons_test")));

        return services.BuildServiceProvider();
    }

    private IServiceProvider GetConsumerServices(int number)
    {
        var services = new ServiceCollection();

        services
            .AutoRegisterHandlersFromAssemblyOf<SimpleMsg>()
            .AddSingleton(_loggerFactory.CreateLogger("Consumer"))
            .AddRebus((configure, services) => configure
                .Logging(l => l.MicrosoftExtensionsLogging(_loggerFactory))
                .Options(o =>
                {
                    o.SetBusName($"Consumer {number}");
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);
                })
                .Transport(t => t.UseNngConsumer("inproc://prodcons_test")));

        return services.BuildServiceProvider();
    }

    private IServiceProvider GetReplyServices()
    {
        var services = new ServiceCollection();

        services
            .AutoRegisterHandlersFromAssemblyOf<RequestMsg>()
            .AddSingleton(_loggerFactory.CreateLogger("Reply"))
            .AddRebus((configure, services) => configure
                .Logging(l => l.MicrosoftExtensionsLogging(_loggerFactory))
                .Options(o =>
                {
                    o.SetBusName("Reply");
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);
                })
                .Transport(t => t.UseNngReply("inproc://repreq_test")));

        return services.BuildServiceProvider();
    }

    private IServiceProvider GetRequestServices(int number)
    {
        var services = new ServiceCollection();

        services
            .AddSingleton(_loggerFactory.CreateLogger("Request"))
            .AddRebus((configure, services) => configure
                .Logging(l => l.MicrosoftExtensionsLogging(_loggerFactory))
                .Options(o =>
                {
                    o.SetBusName($"Request {number}");
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);
                    o.EnableSynchronousRequestReply();
                })
                .Transport(t => t.UseNngRequest("inproc://repreq_test"))
                .Routing(r => r.TypeBased().MapAssemblyNamespaceOf<RequestMsg>(NngTransport.DestinationAny)));

        return services.BuildServiceProvider();
    }

    private IServiceProvider GetPublisherServices()
    {
        var services = new ServiceCollection();

        services
            .AddSingleton(_loggerFactory.CreateLogger("Publisher"))
            .AddRebus((configure, services) => configure
                .Logging(l => l.MicrosoftExtensionsLogging(_loggerFactory))
                .Options(o =>
                {
                    o.SetBusName("Publisher");
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);
                })
                .Transport(t => t.UseNngPublisher("inproc://pubsub_test")));

        return services.BuildServiceProvider();
    }

    private IServiceProvider GetSubscriberServices(int number)
    {
        var services = new ServiceCollection();

        services
            .AutoRegisterHandlersFromAssemblyOf<SimpleMsg>()
            .AddSingleton(_loggerFactory.CreateLogger("Subscriber"))
            .AddRebus((configure, services) => configure
                .Logging(l => l.MicrosoftExtensionsLogging(_loggerFactory))
                .Options(o =>
                {
                    o.SetBusName($"Subscriber {number}");
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);
                })
                .Transport(t => t.UseNngSubscriber("inproc://pubsub_test")));

        return services.BuildServiceProvider();
    }
}