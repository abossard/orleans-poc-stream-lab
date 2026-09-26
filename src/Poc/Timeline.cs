using System.Diagnostics;

namespace Poc;

public sealed record Entry(double T, string Source, string Grain, string Kind, string Detail, int? Version);

/// <summary>Every consumer callback, handshake probe and driver step, in order. The driver asserts on this.</summary>
public sealed class Timeline
{
    private readonly object gate = new();
    private readonly List<Entry> entries = [];
    private readonly Stopwatch clock = Stopwatch.StartNew();

    public event Action<Entry>? Added;

    public void Add(string source, string grain, string kind, string detail = "", int? version = null)
    {
        Entry entry;
        lock (gate)
        {
            entry = new Entry(Math.Round(clock.Elapsed.TotalSeconds, 3), source, grain, kind, detail, version);
            entries.Add(entry);
        }

        Added?.Invoke(entry);
    }

    public void Restart()
    {
        lock (gate)
        {
            entries.Clear();
            clock.Restart();
        }
    }

    public List<Entry> Snapshot()
    {
        lock (gate)
        {
            return [.. entries];
        }
    }

    public bool Has(string grain, string kind, int? version = null) =>
        Snapshot().Any(e => e.Grain == grain && e.Kind == kind && (version is null || e.Version == version));
}
