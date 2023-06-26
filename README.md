# Rebus.nng

[![install from nuget](https://img.shields.io/nuget/v/Rebus.nng.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.nng)

Provides a [NNG](https://nng.nanomsg.org/) (nanomsg-next-gen) transport for [Rebus](https://github.com/rebus-org/Rebus). 

The library implements three patterns from the [NNG](https://nanomsg.org/gettingstarted/):
- Producer/Consumer
- Request/Reply
- Publisher/Subscriber

Check out these links:
- [NNG GitHub](https://github.com/nanomsg/nng)
- [NNG .NET Wrapper](https://github.com/jeikabu/nng.NETCore)

Check the Rebus.ng.TestConsole project for sample usage.

### Producer
```csharp
        services
            .AddRebus((configure, services) => configure
                .Transport(t => t.UseNngProducer("inproc://prodcons_test")));
```

### Consumer
```csharp
        services
            .AddRebus((configure, services) => configure
                .Transport(t => t.UseNngConsumer("inproc://prodcons_test")));
```

### Request
```csharp
        services
            .AddRebus((configure, services) => configure
                .Transport(t => t.UseNngRequest("inproc://repreq_test")));
```

### Reply
```csharp
        services
            .AddRebus((configure, services) => configure
                .Transport(t => t.UseNngReply("inproc://repreq_test")));
```

### Publisher
```csharp
        services
            .AddRebus((configure, services) => configure
                .Transport(t => t.UseNngPublisher("inproc://pubsub_test")));
```

### Subscriber
```csharp
        services
            .AddRebus((configure, services) => configure
                .Transport(t => t.UseNngSubscriber("inproc://pubsub_test")));
```