namespace MicroDb;

public static class Utils
{
    public static void Require(bool cond, string usage)
    {
        if (!cond) throw new ArgumentException("Usage: " + usage);
    }

    public static long ParseSize(string s)
    {
        s = s.Trim().ToLowerInvariant();
        
        // Handle common size suffixes
        if (s.EndsWith("gb")) return (long)(double.Parse(s[..^2]) * (1L << 30));
        if (s.EndsWith("mb")) return (long)(double.Parse(s[..^2]) * (1L << 20));
        if (s.EndsWith("kb")) return (long)(double.Parse(s[..^2]) * 1024L);
        if (s.EndsWith("g")) return (long)(double.Parse(s[..^1]) * (1L << 30));
        if (s.EndsWith("m")) return (long)(double.Parse(s[..^1]) * (1L << 20));
        if (s.EndsWith("k")) return (long)(double.Parse(s[..^1]) * 1024L);
        
        return long.Parse(s);
    }

    public static string FormatSize(long bytes)
    {
        const double KB = 1024, MB = KB * 1024, GB = MB * 1024;
        if (bytes >= GB) return $"{bytes / GB:0.##} GiB";
        if (bytes >= MB) return $"{bytes / MB:0.##} MiB";
        if (bytes >= KB) return $"{bytes / KB:0.##} KiB";
        return $"{bytes} B";
    }

    public static string Preview(string? s) =>
        s is null ? "null" : (s.Length <= 32 ? s : s[..29] + "...");
}