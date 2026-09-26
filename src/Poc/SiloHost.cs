using Azure.Data.Tables;
using Azure.Messaging.EventHubs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Streams;

namespace Poc;

/// <summary>Prod values scaled down so one scenario takes well under a minute.</summary>
public static class Timings
{
    public static readonly TimeSpan DataMinTimeInCache = TimeSpan.FromSeconds(1);      // prod 10 s
    public static readonly TimeSpan DataMaxAgeInCache = TimeSpan.FromSeconds(3);       // prod 30 s
    public static readonly TimeSpan MetadataMinTimeInCache = TimeSpan.FromSeconds(5);  // prod 10 min (default, not overridden)
    public static readonly TimeSpan StreamInactivityPeriod = TimeSpan.FromSeconds(20); // prod 30 min (default)
    public static readonly TimeSpan CollectionAge = TimeSpan.FromSeconds(10);          // prod 15 min (default)
    public static readonly TimeSpan CollectionQuantum = TimeSpan.FromSeconds(2);       // prod 1 min (default)
    public static readonly TimeSpan PingInterval = TimeSpan.FromSeconds(3);            // prod: discovery GetConfig every 5 min
    public static readonly TimeSpan FillerInterval = TimeSpan.FromMilliseconds(500);   // prod: other entities on the partition
    public static readonly TimeSpan CheckpointPersistInterval = TimeSpan.FromSeconds(1);

    // No event on S for longer than StreamInactivityPeriod + cleanup cadence (StreamInactivityPeriod / 10).
    public static readonly TimeSpan QuietPeriod = StreamInactivityPeriod + StreamInactivityPeriod / 10 + TimeSpan.FromSeconds(4);

    // Longer than eviction + MetadataMinTimeInCache (+ its purge cadence), shorter than StreamInactivityPeriod.
    public static readonly TimeSpan WarmQuietPeriod = TimeSpan.FromSeconds(12);

    public static Dictionary<string, double> Describe() => new()
    {
        [nameof(DataMinTimeInCache)] = DataMinTimeInCache.TotalSeconds,
        [nameof(DataMaxAgeInCache)] = DataMaxAgeInCache.TotalSeconds,
        [nameof(MetadataMinTimeInCache)] = MetadataMinTimeInCache.TotalSeconds,
        [nameof(StreamInactivityPeriod)] = StreamInactivityPeriod.TotalSeconds,
        [nameof(CollectionAge)] = CollectionAge.TotalSeconds,
        [nameof(CollectionQuantum)] = CollectionQuantum.TotalSeconds,
        [nameof(PingInterval)] = PingInterval.TotalSeconds,
        [nameof(FillerInterval)] = FillerInterval.TotalSeconds,
        [nameof(QuietPeriod)] = QuietPeriod.TotalSeconds,
        [nameof(WarmQuietPeriod)] = WarmQuietPeriod.TotalSeconds,
        [nameof(CheckpointPersistInterval)] = CheckpointPersistInterval.TotalSeconds,
        ["Partitions"] = 1,
    };
}

public static class EventHubEmulator
{
    // Well-known emulator credentials, see https://learn.microsoft.com/azure/event-hubs/test-locally-with-event-hub-emulator
    public const string ConnectionString =
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
    public const string EventHubName = "poc-hub";
    public const string ConsumerGroup = "orleans";
    public const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    public const string CheckpointTable = "pocCheckpoints";
}

public static class SiloHost
{
    public static IHost Build(string transport, Timeline timeline, string serviceId, int portOffset, TimeSpan metadataMinTimeInCache, TextWriter logFile)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddFilter(TimelineLoggerProvider.PullingAgentCategory, LogLevel.Debug);
        builder.Logging.AddProvider(new TimelineLoggerProvider(timeline, logFile));
        builder.Services.AddSingleton(timeline);

        builder.UseOrleans(silo =>
        {
            silo.UseLocalhostClustering(siloPort: 11111 + portOffset, gatewayPort: 30000 + portOffset, serviceId: serviceId, clusterId: serviceId);
            silo.Configure<GrainCollectionOptions>(o =>
            {
                o.CollectionQuantum = Timings.CollectionQuantum;
                o.CollectionAge = Timings.CollectionAge;
            });
            silo.AddIncomingGrainCallFilter<HandshakeProbe>();

            switch (transport)
            {
                case "memory":
                    silo.AddMemoryStreams(Names.Provider, c =>
                    {
                        c.ConfigurePartitioning(1);
                        ConfigureLikeProd(c, metadataMinTimeInCache);
                    });
                    break;
                case "eventhub":
                    var tableServiceClient = new TableServiceClient(EventHubEmulator.AzuriteConnectionString);
                    silo.AddEventHubStreams(Names.Provider, c =>
                    {
                        c.ConfigureEventHub(ob => ob.Configure(o =>
                            o.ConfigureEventHubConnection(
                                new EventHubConnection(EventHubEmulator.ConnectionString, EventHubEmulator.EventHubName),
                                EventHubEmulator.ConsumerGroup)));
                        c.UseAzureTableCheckpointer(ob => ob.Configure(o =>
                        {
                            o.TableServiceClient = tableServiceClient;
                            o.TableName = EventHubEmulator.CheckpointTable;
                            o.PersistInterval = Timings.CheckpointPersistInterval;
                        }));
                        ConfigureLikeProd(c, metadataMinTimeInCache);
                    });
                    break;
                default:
                    throw new ArgumentException($"Unknown transport '{transport}'");
            }
        });

        return builder.Build();
    }

    // Same knobs as Monitor.HealthIntelligence.Insights.Silo/Program.cs L223-243, plus scaled values for the defaults prod relies on.
    private static void ConfigureLikeProd(ISiloRecoverableStreamConfigurator c, TimeSpan metadataMinTimeInCache)
    {
        c.ConfigureCacheEviction(ob => ob.Configure(o =>
        {
            o.DataMinTimeInCache = Timings.DataMinTimeInCache;
            o.DataMaxAgeInCache = Timings.DataMaxAgeInCache;
            o.MetadataMinTimeInCache = metadataMinTimeInCache;
        }));
        c.ConfigurePullingAgent(ob => ob.Configure(o => o.StreamInactivityPeriod = Timings.StreamInactivityPeriod));
        c.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
    }
}
