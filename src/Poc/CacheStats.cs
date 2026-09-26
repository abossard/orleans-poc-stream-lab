using System.Diagnostics.Metrics;

namespace Poc;

/// <summary>Samples the pooled cache's message count and block size from the Orleans cache monitor instruments (DefaultCacheMonitor).</summary>
public sealed class CacheStats : IDisposable
{
    private const string Length = "orleans-streams-queue-cache-length";
    private const string Size = "orleans-streams-queue-cache-size";

    private readonly MeterListener listener = new();
    private readonly Timer sampler;
    private long messages;
    private long bytes;
    private long maxMessages;
    private long maxBytes;

    public CacheStats()
    {
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name is Length or Size)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == Length)
            {
                Interlocked.Exchange(ref messages, value);
                InterlockedMax(ref maxMessages, value);
            }
            else
            {
                Interlocked.Exchange(ref bytes, value);
                InterlockedMax(ref maxBytes, value);
            }
        });
        listener.Start();
        sampler = new Timer(_ => listener.RecordObservableInstruments(), null, TimeSpan.Zero, TimeSpan.FromMilliseconds(250));
    }

    public long Messages => Interlocked.Read(ref messages);

    public long MaxMessages => Interlocked.Read(ref maxMessages);

    public long MaxBytes => Interlocked.Read(ref maxBytes);

    /// <summary>Forget the warm-up peak.</summary>
    public void Reset()
    {
        Interlocked.Exchange(ref maxMessages, Messages);
        Interlocked.Exchange(ref maxBytes, Interlocked.Read(ref bytes));
    }

    public void Dispose()
    {
        sampler.Dispose();
        listener.Dispose();
    }

    private static void InterlockedMax(ref long target, long value)
    {
        long current;
        while (value > (current = Interlocked.Read(ref target)) && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }
}
