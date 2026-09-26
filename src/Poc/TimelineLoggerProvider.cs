using Microsoft.Extensions.Logging;

namespace Poc;

/// <summary>Writes all logs to a file and copies pulling-agent lines about consumer streams into the timeline.</summary>
public sealed class TimelineLoggerProvider(Timeline timeline, TextWriter file) : ILoggerProvider
{
    /// <summary>PersistentStreamPullingAgent logs as "{namespace}.{streamProviderName}".</summary>
    public const string PullingAgentCategory = "Orleans.Streams." + Names.Provider;

    private readonly object gate = new();

    public ILogger CreateLogger(string categoryName) => new Logger(this, categoryName);

    public void Dispose() => file.Flush();

    private void Write(string category, LogLevel level, string message, Exception? exception)
    {
        lock (gate)
        {
            file.WriteLine($"{DateTime.UtcNow:HH:mm:ss.fff} {level,-11} {category}: {message}{(exception is null ? "" : " | " + exception.GetType().Name + ": " + exception.Message)}");
        }

        if (category == PullingAgentCategory && message.Contains(Names.ConsumerNamespace, StringComparison.Ordinal))
        {
            timeline.Add("agent", "", "AgentLog", message);
        }
    }

    private sealed class Logger(TimelineLoggerProvider owner, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            owner.Write(category, logLevel, formatter(state, exception), exception);
    }
}
