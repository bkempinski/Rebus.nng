using Rebus.Messages;
using System.Collections.Generic;

namespace Rebus.Models;

internal record TransportMessageInternal
{
    public Dictionary<string, string> Headers { get; set; } = new();

    public byte[] Body { get; set; } = new byte[0];

    public TransportMessageInternal() { }
    public TransportMessageInternal(TransportMessage transportMessage) =>
        (Headers, Body) = (transportMessage.Headers, transportMessage.Body);
}