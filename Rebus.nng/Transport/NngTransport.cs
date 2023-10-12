using nng;
using nng.Native;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Models;
using Rebus.Subscriptions;
using Rebus.Transport;
using System;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using static nng.Native.Defines;

namespace Rebus.nng.Transport;

public class NngTransport : ITransport, ISubscriptionStorage, IInitializable, IDisposable
{
    public static string DestinationAny = "";

    private static readonly byte[] destinationMagicBytes = { 0x1A, 0x2B, 0x3C, 0x4D };
    private static readonly string[] nngSchemes = { "inproc", "ipc", "tcp", "tcp4", "tcp6", "ws", "wss" };

    private readonly NngPattern _nngPattern;
    private readonly string _nngUrl;
    private readonly NngTransportOptions _options;

    private static readonly object _lockObject = new object();
    private static IAPIFactory<INngMsg> _nngApiFactory;

    private INngSocket _nngSocket = null;
    private INngListener _nngListener = null;
    private INngDialer _nngDialer = null;

    public string Address =>
        _nngPattern switch
        {
            NngPattern.Consumer => "",
            NngPattern.Reply => "",
            NngPattern.Subscriber => "",
            _ => null
        };

    public bool IsCentralized => true;

    public NngTransport
        (
            NngPattern nngPattern,
            string nngUrl,
            NngTransportOptions options
        ) => (_nngPattern, _nngUrl, _options)
            = (nngPattern, ValidateNngUrl(nngUrl), options);

    public void CreateQueue(string address) { }

    public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        var request = await RetryAgain(() => (_nngSocket as IRecvSocket).RecvMsg(NngFlag.NNG_FLAG_NONBLOCK), cancellationToken);

        if (request.IsOk())
            using (var nngMsg = request.Unwrap())
                return DecomposeNngMessage(nngMsg);

        return null;
    }

    public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
    {
        if (_nngPattern == NngPattern.Request)
            message.Headers.Add(Headers.ReturnAddress, "");

        using (var cts = new CancellationTokenSource())
        using (var nngMsg = ComposeNngMessage(destinationAddress, message))
        {
            cts.CancelAfter(_options.SendTimeout);
            await RetryAgain(() => (_nngSocket as ISendSocket).SendMsg(nngMsg), cts.Token);
        }
    }

    public Task<string[]> GetSubscriberAddresses(string topic)
    {
        return Task.FromResult(new[] { topic });
    }

    public Task RegisterSubscriber(string topic, string subscriberAddress)
    {
        var nnRes = _nngSocket.SetOpt(NNG_OPT_SUB_SUBSCRIBE, Encoding.UTF8.GetBytes(topic));

        if (nnRes != 0)
            throw new ArgumentException($"Cannot subscribe to the topic \"{topic}\" - {(NngErrno)nnRes}", nameof(topic));

        return Task.CompletedTask;
    }

    public Task UnregisterSubscriber(string topic, string subscriberAddress)
    {
        var nnRes = _nngSocket.SetOpt(NNG_OPT_SUB_UNSUBSCRIBE, Encoding.UTF8.GetBytes(topic));

        if (nnRes != 0)
            throw new ArgumentException($"Cannot unsubscribe from the topic \"{topic}\" - {(NngErrno)nnRes}", nameof(topic));

        return Task.CompletedTask;
    }

    public void Initialize()
    {
        lock (_lockObject)
            if (_nngApiFactory == null)
                _nngApiFactory = NngLoadContext.Init(_options.OwnAssemblyLoadContext ?? new NngLoadContext(_options.NngPath));

        // Create socket
        switch (_nngPattern)
        {
            case NngPattern.Producer:
                _nngSocket = _nngApiFactory.PusherOpen().Unwrap();
                _nngListener = _nngSocket.ListenerCreate(_nngUrl).Unwrap();
                break;
            case NngPattern.Consumer:
                _nngSocket = _nngApiFactory.PullerOpen().Unwrap();
                _nngDialer = _nngSocket.DialerCreate(_nngUrl).Unwrap();
                break;
            case NngPattern.Reply:
                _nngSocket = _nngApiFactory.ReplierOpen().Unwrap();
                _nngListener = _nngSocket.ListenerCreate(_nngUrl).Unwrap();
                break;
            case NngPattern.Request:
                _nngSocket = _nngApiFactory.RequesterOpen().Unwrap();
                _nngDialer = _nngSocket.DialerCreate(_nngUrl).Unwrap();
                break;
            case NngPattern.Publisher:
                _nngSocket = _nngApiFactory.PublisherOpen().Unwrap();
                _nngListener = _nngSocket.ListenerCreate(_nngUrl).Unwrap();
                break;
            case NngPattern.Subscriber:
                _nngSocket = _nngApiFactory.SubscriberOpen().Unwrap();
                _nngDialer = _nngSocket.DialerCreate(_nngUrl).Unwrap();
                break;
            default:
                throw new ArgumentException($"Unsupported NNG pattern: {_nngPattern}", nameof(_nngPattern));
        }

        var setOptions = (IOptions)_nngListener ?? _nngDialer;
        var result = 0;

        // Set options
        if (_options.NngSetOptions != null && _options.NngSetOptions.Any())
            foreach (var setOption in _options.NngSetOptions)
            {
                switch (setOption.Value)
                {
                    case byte[] data:
                        result = setOptions.SetOpt(setOption.Key, data);
                        break;
                    case bool data:
                        result = setOptions.SetOpt(setOption.Key, data);
                        break;
                    case int data:
                        result = setOptions.SetOpt(setOption.Key, data);
                        break;
                    case nng_duration data:
                        result = setOptions.SetOpt(setOption.Key, data);
                        break;
                    case IntPtr data:
                        result = setOptions.SetOpt(setOption.Key, data);
                        break;
                    case UIntPtr data:
                        result = setOptions.SetOpt(setOption.Key, data);
                        break;
                    case string data:
                        result = setOptions.SetOpt(setOption.Key, data);
                        break;
                    case ulong data:
                        result = setOptions.SetOpt(setOption.Key, data);
                        break;
                    default:
                        throw new ArgumentException($"Unsupported type for nng set option - {setOption.Key}");
                }

                if (result != 0)
                    throw new ArgumentException($"Cannot set nng option {setOption.Key} to value {setOption.Value} ({result})");
            }

        // Listen/Dial
        if (_nngListener != null)
            result = _nngListener.Start();
        else if (_nngDialer != null)
            result = _nngDialer.Start();
        else
            throw new ArgumentException($"Unsupported NNG pattern: {_nngPattern}", nameof(_nngPattern));

        if (result != 0)
            throw new ArgumentException($"Cannot initialize NNG connection: {_nngPattern} ({result})", nameof(_nngPattern));
    }

    public void Dispose()
    {
        if (_nngSocket != null)
            _nngSocket.Dispose();
    }

    private INngMsg ComposeNngMessage(string destinationAddress, TransportMessage message)
    {
        var nngMsg = _nngApiFactory.CreateMessage();

        if (!string.IsNullOrEmpty(destinationAddress))
        {
            nngMsg.Append(Encoding.UTF8.GetBytes(destinationAddress));
            nngMsg.Append(destinationMagicBytes);
        }

        nngMsg.Append(JsonSerializer.SerializeToUtf8Bytes(new TransportMessageInternal(message), _options.JsonSerializerOptions));

        return nngMsg;
    }

    private TransportMessage DecomposeNngMessage(INngMsg nngMsg)
    {
        var bytes = nngMsg.AsSpan();
        var destinationMagicBytesPos = bytes.IndexOf(destinationMagicBytes);
        Utf8JsonReader reader;

        if (destinationMagicBytesPos >= 0)
            reader = new Utf8JsonReader(bytes.Slice(destinationMagicBytesPos + destinationMagicBytes.Length));
        else
            reader = new Utf8JsonReader(bytes);

        var message = JsonSerializer.Deserialize<TransportMessageInternal>(ref reader, _options.JsonSerializerOptions);
        return new TransportMessage(message.Headers, message.Body);
    }

    private string ValidateNngUrl(string nngUrl)
    {
        var uri = new Uri(nngUrl);

        if (!nngSchemes.Contains(uri.Scheme.ToLower()))
            throw new ArgumentException($"Protocol not supported: {uri.Scheme}", nameof(nngUrl));

        return nngUrl;
    }

    private static async Task<NngResult<T>> RetryAgain<T>(Func<NngResult<T>> func, CancellationToken stoppingToken)
    {
        NngResult<T> res = func();

        while ((res.IsErr(NngErrno.EAGAIN) || res.IsErr(NngErrno.ESTATE)) && !stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(50, stoppingToken);
            res = func();
        }

        return res;
    }
}