using System.Reflection;
using Orleans.Streams;
using Orleans.Streams.Core;

namespace Poc;

public static class Names
{
    public const string Provider = "poc";
    public const string ConsumerNamespace = "entity-update";
    public const string FillerNamespace = "filler";
}

[GenerateSerializer]
public sealed record Payload([property: Id(0)] int Version, [property: Id(1)] int ProcessingDelayMs = 0);

public interface IConsumerGrain : IGrainWithStringKey
{
    /// <summary>Like EntityConfigGrain.Update: publish on the grain's own stream.</summary>
    Task Update(int version);

    /// <summary>Like EntityConfigGrain.GetConfig: a read that keeps the activation alive and publishes nothing.</summary>
    Task<int> Ping();
}

/// <summary>Mirrors EntityConfigGrain: implicit subscription, ResumeAsync in OnSubscribed, OnErrorAsync only records.</summary>
[ImplicitStreamSubscription(Names.ConsumerNamespace)]
public sealed class ConsumerGrain(Timeline timeline) : Grain, IConsumerGrain, IStreamSubscriptionObserver, IAsyncObserver<Payload>
{
    private int lastVersion;

    private string Key => this.GetPrimaryKeyString();

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        timeline.Add("grain", Key, "Activated");
        return Task.CompletedTask;
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        timeline.Add("grain", Key, "Deactivated", reason.ReasonCode.ToString());
        return Task.CompletedTask;
    }

    public Task OnSubscribed(IStreamSubscriptionHandleFactory handleFactory)
    {
        timeline.Add("grain", Key, "OnSubscribed");
        return handleFactory.Create<Payload>().ResumeAsync(this);
    }

    public async Task OnNextAsync(Payload item, StreamSequenceToken? token = null)
    {
        timeline.Add("grain", Key, "OnNextAsync", $"token={token}", item.Version);
        lastVersion = item.Version;
        if (item.ProcessingDelayMs > 0)
        {
            await Task.Delay(item.ProcessingDelayMs);
        }
    }

    public Task OnErrorAsync(Exception ex)
    {
        timeline.Add("grain", Key, "OnErrorAsync", $"{ex.GetType().FullName}: {ex.Message}");
        return Task.CompletedTask;
    }

    public Task OnCompletedAsync() => Task.CompletedTask;

    public async Task Update(int version)
    {
        var stream = this.GetStreamProvider(Names.Provider)
            .GetStream<Payload>(StreamId.Create(Names.ConsumerNamespace, Key));
        await stream.OnNextAsync(new Payload(version));
        timeline.Add("grain", Key, "Published", "", version);
    }

    public Task<int> Ping() => Task.FromResult(lastVersion);
}

/// <summary>Records the expectedToken (StreamHandshakeToken) the grain returns from GetSequenceToken() during the handshake.</summary>
public sealed class HandshakeProbe(Timeline timeline) : IIncomingGrainCallFilter
{
    public async Task Invoke(IIncomingGrainCallContext context)
    {
        await context.Invoke();
        // IStreamConsumerExtension.GetSequenceToken and StreamHandshakeToken are internal, so match by name.
        if (context.InterfaceMethod?.Name != "GetSequenceToken")
        {
            return;
        }

        var result = context.Result;
        var detail = result is null
            ? "returned null (no expectedToken)"
            : $"returned {result.GetType().Name}({result.GetType().GetProperty("Token", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)?.GetValue(result)})";
        timeline.Add("agent->grain", context.TargetId.Key.ToString()!, "GetSequenceToken", detail);
    }
}
