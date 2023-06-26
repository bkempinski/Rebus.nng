using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using nng;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Transport;
using System;
using System.IO;
using System.Linq;
using System.Text;
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
    private readonly JsonSerializer _jsonSerializer;

    private static readonly object _lockObject = new object();
    private static IAPIFactory<INngMsg> _nngApiFactory;

    private INngSocket _nngSocket;

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
        ) => (_nngPattern, _nngUrl, _options, _jsonSerializer)
            = (nngPattern, ValidateNngUrl(nngUrl), options, new JsonSerializer());

    public void CreateQueue(string address) { }

    public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        var request = await RetryAgain(() => (_nngSocket as IRecvSocket).RecvMsg(NngFlag.NNG_FLAG_NONBLOCK), cancellationToken);

        if (request.IsOk())
            using (var nngMsg = request.Unwrap())
                return DecomposeNngMessage(nngMsg).Message;

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

            await RetryAgain(() => (_nngSocket as ISendSocket).SendMsg(nngMsg, NngFlag.NNG_FLAG_NONBLOCK), cts.Token);
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

        switch (_nngPattern)
        {
            case NngPattern.Producer:
                _nngSocket = _nngApiFactory.PusherOpen().ThenListen(_nngUrl).Unwrap();
                break;
            case NngPattern.Consumer:
                _nngSocket = _nngApiFactory.PullerOpen().ThenDial(_nngUrl).Unwrap();
                break;
            case NngPattern.Reply:
                _nngSocket = _nngApiFactory.ReplierOpen().ThenListen(_nngUrl).Unwrap();
                break;
            case NngPattern.Request:
                _nngSocket = _nngApiFactory.RequesterOpen().ThenDial(_nngUrl).Unwrap();
                break;
            case NngPattern.Publisher:
                _nngSocket = _nngApiFactory.PublisherOpen().ThenListen(_nngUrl).Unwrap();
                break;
            case NngPattern.Subscriber:
                _nngSocket = _nngApiFactory.SubscriberOpen().ThenDial(_nngUrl).Unwrap();
                break;
            default:
                throw new ArgumentException($"Unsupported NNG pattern: {_nngPattern}", nameof(_nngPattern));
        }
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

        using (var ms = new MemoryStream())
        using (var bson = new BsonDataWriter(ms))
        {
            _jsonSerializer.Serialize(bson, message);
            nngMsg.Append(ms.ToArray());
        }

        return nngMsg;
    }

    private (string DestinationAddress, TransportMessage Message) DecomposeNngMessage(INngMsg nngMsg)
    {
        var bytes = nngMsg.AsSpan();
        var bytesArray = nngMsg.AsSpan().ToArray();
        var destinationAddress = (string)null;
        var destinationMagicBytesPos = bytes.IndexOf(destinationMagicBytes);

        if (destinationMagicBytesPos >= 0)
            destinationAddress = Encoding.UTF8.GetString(bytesArray, 0, destinationMagicBytesPos);

        using (var ms = new MemoryStream(bytesArray))
        using (var br = new BinaryReader(ms))
        using (var bson = new BsonDataReader(br))
        {
            if (destinationMagicBytesPos >= 0)
                br.BaseStream.Position = destinationMagicBytesPos + destinationMagicBytes.Length;

            return (destinationAddress, _jsonSerializer.Deserialize<TransportMessage>(bson));
        }
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