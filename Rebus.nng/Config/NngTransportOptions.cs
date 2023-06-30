using System;
using System.IO;
using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json;

namespace Rebus.Config;

public class NngTransportOptions
{
    public string NngPath { get; set; } = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

    public AssemblyLoadContext OwnAssemblyLoadContext { get; set; } = null;

    public JsonSerializerOptions JsonSerializerOptions { get; set; } = null;

    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(5);
}