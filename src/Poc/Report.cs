using System.Text;
using System.Text.Json;

namespace Poc;

public sealed record RunReport(
    string OrleansVersion,
    Dictionary<string, string> Assemblies,
    string Transport,
    DateTime StartedUtc,
    Dictionary<string, double> Timings,
    List<ScenarioResult> Scenarios);

public static class Report
{
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };

    public static void Write(RunReport run, string outDir)
    {
        var name = Path.Combine(outDir, $"{run.OrleansVersion}-{run.Transport}");
        File.WriteAllText(name + ".json", JsonSerializer.Serialize(run, Json));
        File.WriteAllText(name + ".md", Markdown(run));
    }

    public static void WriteSummary(string outDir)
    {
        var runs = Directory.GetFiles(outDir, "*.json")
            .Select(f => JsonSerializer.Deserialize<RunReport>(File.ReadAllText(f))!)
            .ToList();
        var sb = new StringBuilder();
        sb.AppendLine("# Results summary");
        sb.AppendLine();
        sb.AppendLine("| Scenario | Transport | Orleans | OnErrorAsync | Delivered | Lost | Expected | Result |");
        sb.AppendLine("|---|---|---|---|---|---|---|---|");
        foreach (var (run, s) in runs
                     .SelectMany(r => r.Scenarios.Select(s => (r, s)))
                     .OrderBy(x => x.s.Id).ThenBy(x => x.r.Transport).ThenBy(x => x.r.OrleansVersion))
        {
            var errors = s.OnErrorCount == 0 ? "0" : $"{s.OnErrorCount} ({string.Join(", ", s.ErrorTypes)})";
            sb.AppendLine($"| {s.Id} | {run.Transport} | {run.OrleansVersion} | {errors} | {s.Delivered.Length}/{s.Published.Length} | {Range(s.Lost)} | {Cell(s.Expected)} | {(s.Pass ? "PASS" : "FAIL")} |");
        }

        File.WriteAllText(Path.Combine(outDir, "summary.md"), sb.ToString());
        Console.Write(sb);
    }

    private static string Markdown(RunReport run)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"# Orleans {run.OrleansVersion}, transport {run.Transport}");
        sb.AppendLine();
        sb.AppendLine($"- Started (UTC): {run.StartedUtc:O}");
        sb.AppendLine($"- Loaded assemblies: {string.Join(", ", run.Assemblies.Select(a => $"{a.Key} {a.Value}"))}");
        sb.AppendLine($"- Timings (s): {string.Join(", ", run.Timings.Select(t => $"{t.Key}={t.Value}"))}");
        sb.AppendLine();
        sb.AppendLine("| Scenario | Expected | Observed | Result |");
        sb.AppendLine("|---|---|---|---|");
        foreach (var s in run.Scenarios)
        {
            sb.AppendLine($"| {s.Id} | {Cell(s.Expected)} | {Cell(s.Observed)} | {(s.Pass ? "PASS" : "FAIL")} |");
        }

        foreach (var s in run.Scenarios)
        {
            sb.AppendLine();
            sb.AppendLine($"## {s.Id}: {s.Title}");
            sb.AppendLine();
            sb.AppendLine($"- Expected: {s.Expected}");
            sb.AppendLine($"- Observed: {s.Observed}");
            sb.AppendLine($"- Result: {(s.Pass ? "PASS" : "FAIL")}");
            sb.AppendLine();
            sb.AppendLine("| t (s) | Source | Grain | Kind | Version | Detail |");
            sb.AppendLine("|---:|---|---|---|---:|---|");
            foreach (var e in s.Timeline)
            {
                sb.AppendLine($"| {e.T:0.000} | {e.Source} | {e.Grain} | {e.Kind} | {e.Version} | {Cell(e.Detail)} |");
            }
        }

        return sb.ToString();
    }

    private static string Range(int[] values) => values.Length switch
    {
        0 => "0",
        _ when values.Length > 1 && values[^1] - values[0] == values.Length - 1 => $"{values.Length} ({values[0]}..{values[^1]})",
        _ => $"{values.Length} ({string.Join(",", values)})",
    };

    private static string Cell(string text) => text.Replace("|", "\\|").Replace("\n", " ").Replace("\r", "");
}
