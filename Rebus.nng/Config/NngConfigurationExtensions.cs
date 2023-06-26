using Rebus.nng.Transport;
using Rebus.Subscriptions;
using Rebus.Transport;
using System;

namespace Rebus.Config;

public static class NngConfigurationExtensions
{
    public static void UseNngProducer(this StandardConfigurer<ITransport> configurer, string nngUrl, NngTransportOptions options = null)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (nngUrl == null) throw new ArgumentNullException(nameof(nngUrl));

        Register(configurer, NngPattern.Producer, nngUrl, options);

        OneWayClientBackdoor.ConfigureOneWayClient(configurer);
    }

    public static void UseNngConsumer(this StandardConfigurer<ITransport> configurer, string nngUrl, NngTransportOptions options = null)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (nngUrl == null) throw new ArgumentNullException(nameof(nngUrl));

        Register(configurer, NngPattern.Consumer, nngUrl, options);
    }

    public static void UseNngReply(this StandardConfigurer<ITransport> configurer, string nngUrl, NngTransportOptions options = null)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (nngUrl == null) throw new ArgumentNullException(nameof(nngUrl));

        Register(configurer, NngPattern.Reply, nngUrl, options);
    }

    public static void UseNngRequest(this StandardConfigurer<ITransport> configurer, string nngUrl, NngTransportOptions options = null)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (nngUrl == null) throw new ArgumentNullException(nameof(nngUrl));

        Register(configurer, NngPattern.Request, nngUrl, options);
    }

    public static void UseNngPublisher(this StandardConfigurer<ITransport> configurer, string nngUrl, NngTransportOptions options = null)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (nngUrl == null) throw new ArgumentNullException(nameof(nngUrl));

        Register(configurer, NngPattern.Publisher, nngUrl, options);

        OneWayClientBackdoor.ConfigureOneWayClient(configurer);
    }

    public static void UseNngSubscriber(this StandardConfigurer<ITransport> configurer, string nngUrl, NngTransportOptions options = null)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (nngUrl == null) throw new ArgumentNullException(nameof(nngUrl));

        Register(configurer, NngPattern.Subscriber, nngUrl, options);
    }

    static void Register(StandardConfigurer<ITransport> configurer, NngPattern nngPattern, string nngUrl, NngTransportOptions optionsOrNull)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));

        var options = optionsOrNull ?? new NngTransportOptions();

        configurer
            .OtherService<NngTransport>()
            .Register(c => new NngTransport(nngPattern, nngUrl, options));

        configurer
            .OtherService<ISubscriptionStorage>()
            .Register(c => c.Get<NngTransport>());

        configurer
            .Register(c => c.Get<NngTransport>());
    }
}