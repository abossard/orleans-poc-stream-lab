using System.Reflection;
using Orleans.Configuration;
using Poc;

var options = ParseArgs(args);
var outDir = Path.GetFullPath(options.GetValueOrDefault("out", "results"));
Directory.CreateDirectory(outDir);

if (options.ContainsKey("summary"))
{
    Report.WriteSummary(outDir);
    return 0;
}

var transport = options.GetValueOrDefault("transport", "memory");
var scenarioIds = options.GetValueOrDefault("scenarios", "A,B,C,D").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

var assemblies = new[] { typeof(IGrain).Assembly, typeof(StreamPullingAgentOptions).Assembly, typeof(EventHubOptions).Assembly }
    .ToDictionary(a => a.GetName().Name!, InformationalVersion);
var orleansVersion = Version.Parse(assemblies["Orleans.Streaming"]);
Console.WriteLine($"Orleans.Streaming {orleansVersion} (loaded from {typeof(StreamPullingAgentOptions).Assembly.Location}), transport {transport}");

var logDir = Path.Combine(outDir, "logs");
Directory.CreateDirectory(logDir);
var runner = new Scenarios(transport, orleansVersion, logDir);
var run = new RunReport(orleansVersion.ToString(), assemblies, transport, DateTime.UtcNow, Timings.Describe(), []);
for (var i = 0; i < scenarioIds.Length; i++)
{
    Console.WriteLine($"  scenario {scenarioIds[i]}: {Scenarios.Titles[scenarioIds[i]]}");
    var result = await runner.Run(scenarioIds[i], i);
    Console.WriteLine($"    expected: {result.Expected}");
    Console.WriteLine($"    observed: {result.Observed}  => {(result.Pass ? "PASS" : "FAIL")}");
    run.Scenarios.Add(result);
}

Report.Write(run, outDir);
return run.Scenarios.All(s => s.Pass) ? 0 : 1;

// Orleans stamps e.g. "10.2.1. Commit Hash: d3c2af3..."; keep the leading version number.
static string InformationalVersion(Assembly a) =>
    System.Text.RegularExpressions.Regex.Match(
        a.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!.InformationalVersion, @"^\d+(\.\d+)+").Value;

static Dictionary<string, string> ParseArgs(string[] args)
{
    var result = new Dictionary<string, string>();
    for (var i = 0; i < args.Length; i++)
    {
        if (!args[i].StartsWith("--"))
        {
            continue;
        }

        var hasValue = i + 1 < args.Length && !args[i + 1].StartsWith("--");
        result[args[i][2..]] = hasValue ? args[++i] : "true";
    }

    return result;
}
