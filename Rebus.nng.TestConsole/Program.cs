using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Rebus.nng.TestConsole.Services;

var host = Host
    .CreateDefaultBuilder(args)
    .ConfigureLogging((hostContext, builder) =>
    {
        builder.ClearProviders();
        builder.SetMinimumLevel(LogLevel.Information);
        builder.AddSimpleConsole(options =>
        {
            options.SingleLine = false;
            options.TimestampFormat = "HH:mm:ss.fff ";
            options.ColorBehavior = LoggerColorBehavior.Enabled;
        });
    })
    .ConfigureServices(services =>
    {
        services.AddHostedService<TestService>();
    })
    .Build();

host.Run();