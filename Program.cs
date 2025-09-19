using System.Diagnostics;
using System.Text;

var dataFile = "database.bin";
var indexFile = "database.idx";

if (args.Length == 0)
{
    Console.Error.WriteLine("""
    Usage:
      dotnet run -- set <key:int> <value>
      dotnet run -- get <key:int>
      dotnet run -- get-scan <key:int>
      dotnet run -- fill [size:2g|...] [minLen] [maxLen]
      dotnet run -- bench [size:2g|...] [minLen] [maxLen] [key?]
    """);
    return;
}

switch (args[0].ToLowerInvariant())
{
    case "set":
        Require(args.Length >= 3, "set <key> <value>");
        Set(int.Parse(args[1]), args[2]);
        break;

    case "get":
        Require(args.Length >= 2, "get <key>");
        GetIndexed(int.Parse(args[1]));
        break;

    case "get-scan":
        Require(args.Length >= 2, "get-scan <key>");
        GetScan(int.Parse(args[1]));
        break;

    case "fill":
    {
        long size = args.Length >= 2 ? ParseSize(args[1]) : (2L << 30);
        int minLen = args.Length >= 3 ? int.Parse(args[2]) : 16;
        int maxLen = args.Length >= 4 ? int.Parse(args[3]) : 64;
        var (entries, lastKey, bytes) = Fill(size, minLen, maxLen);
        Console.Error.WriteLine($"Filled {entries:N0} entries, lastKey={lastKey}, data={FormatSize(bytes)}");
        break;
    }

    case "bench":
    {
        long size = args.Length >= 2 ? ParseSize(args[1]) : (2L << 30);
        int minLen = args.Length >= 3 ? int.Parse(args[2]) : 16;
        int maxLen = args.Length >= 4 ? int.Parse(args[3]) : 64;

        // fresh start
        if (File.Exists(dataFile)) File.Delete(dataFile);
        if (File.Exists(indexFile)) File.Delete(indexFile);

        var sw = Stopwatch.StartNew();
        var (entries, lastKey, bytes) = Fill(size, minLen, maxLen);
        sw.Stop();
        Console.WriteLine($"fill:     {sw.Elapsed.TotalSeconds:F2}s  ({entries:N0} entries, {FormatSize(bytes)})");

        int benchKey = args.Length >= 5 ? int.Parse(args[4]) : lastKey;

        // Warmup (JIT)
        _ = GetIndexedInternal(benchKey);
        _ = IndexCache.Get(indexFile);

        sw.Restart();
        var v1 = GetIndexedInternal(benchKey);
        sw.Stop();
        Console.WriteLine($"get:      {sw.Elapsed.TotalMilliseconds:F1} ms  -> {Preview(v1)}");

        sw.Restart();
        var v2 = GetScanInternal(benchKey);
        sw.Stop();
        Console.WriteLine($"get-scan: {sw.Elapsed.TotalSeconds:F2} s   -> {Preview(v2)}");
        break;
    }

    default:
        Console.Error.WriteLine("Unknown command.");
        break;
}

void Set(int key, string value)
{
    byte[] data = Encoding.UTF8.GetBytes(value);

    using var fs = new FileStream(dataFile, FileMode.Append, FileAccess.Write, FileShare.Read);
    long offset = fs.Position;

    using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
    bw.Write(key);
    bw.Write(data.Length);
    bw.Write(data);
    bw.Flush();

    using var fi = new FileStream(indexFile, FileMode.Append, FileAccess.Write, FileShare.Read);
    using var iw = new BinaryWriter(fi);
    iw.Write(key);
    iw.Write(offset);
    IndexCache.Add(key, offset);   // <— Cache aktuell halten
}

void GetIndexed(int searchKey)
{
    var val = GetIndexedInternal(searchKey);
    if (val is null) Console.Error.WriteLine("Key not found (index).");
    else Console.WriteLine(val);
}

string? GetIndexedInternal(int searchKey)
{
    var index = IndexCache.Get(indexFile);

    if (!index.TryGetValue(searchKey, out var pos))
        return null;

    using var fs = new FileStream(dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
    using var br = new BinaryReader(fs);
    fs.Seek(pos, SeekOrigin.Begin);

    int k = br.ReadInt32();
    int len = br.ReadInt32();
    var buf = br.ReadBytes(len);
    return Encoding.UTF8.GetString(buf);
}

void GetScan(int searchKey)
{
    var val = GetScanInternal(searchKey);
    if (val is null) Console.Error.WriteLine("Key not found (scan).");
    else Console.WriteLine(val);
}

string? GetScanInternal(int searchKey)
{
    string? last = null;
    using var fs = new FileStream(dataFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
    using var br = new BinaryReader(fs);

    while (fs.Position < fs.Length)
    {
        int k = br.ReadInt32();
        int len = br.ReadInt32();
        var buf = br.ReadBytes(len);
        if (k == searchKey)
            last = Encoding.UTF8.GetString(buf);
    }
    return last;
}

(Dictionary<int,long> dict, long bytes) LoadIndexWithSize()
{
    var dict = new Dictionary<int,long>();
    if (!File.Exists(indexFile)) return (dict, 0);
    using var fi = new FileStream(indexFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
    using var br = new BinaryReader(fi);
    while (fi.Position < fi.Length)
    {
        int k = br.ReadInt32();
        long off = br.ReadInt64();
        dict[k] = off;
    }
    return (dict, new FileInfo(indexFile).Length);
}

Dictionary<int,long> LoadIndex()
{
    return LoadIndexWithSize().dict;
}

// ⬇️ Hier war das Problem: kein 'static' vor den Rückgabetyp
(long entries, int lastKey, long dataBytes) Fill(long targetBytes, int minLen, int maxLen)
{
    using var fs = new FileStream(dataFile, FileMode.Append, FileAccess.Write, FileShare.Read, 1 << 20, FileOptions.SequentialScan);
    using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
    using var fi = new FileStream(indexFile, FileMode.Append, FileAccess.Write, FileShare.Read, 1 << 20, FileOptions.SequentialScan);
    using var iw = new BinaryWriter(fi, Encoding.UTF8, leaveOpen: true);

    var rnd = Random.Shared;
    var sb = new StringBuilder(maxLen);
    int key = 1;
    long entries = 0;

    while (fs.Length < targetBytes)
    {
        int len = rnd.Next(minLen, maxLen + 1);
        sb.Clear();
        sb.Append('v').Append(key).Append('-');
        while (sb.Length < len) sb.Append('x');
        byte[] data = Encoding.UTF8.GetBytes(sb.ToString());

        long off = fs.Position;
        bw.Write(key);
        bw.Write(data.Length);
        bw.Write(data);

        iw.Write(key);
        iw.Write(off);

        entries++;
        if ((key & 0x3FFF) == 0) { bw.Flush(); iw.Flush(); }

        key++;
    }

    bw.Flush(); iw.Flush();
    return (entries, key - 1, fs.Length);
}

static void Require(bool cond, string usage)
{
    if (!cond) throw new ArgumentException("Usage: " + usage);
}

static long ParseSize(string s)
{
    s = s.Trim().ToLowerInvariant();
    if (s.EndsWith("g")) return (long)(double.Parse(s[..^1]) * (1L << 30));
    if (s.EndsWith("m")) return (long)(double.Parse(s[..^1]) * (1L << 20));
    if (s.EndsWith("k")) return (long)(double.Parse(s[..^1]) * 1024L);
    return long.Parse(s);
}

static string FormatSize(long bytes)
{
    const double KB = 1024, MB = KB * 1024, GB = MB * 1024;
    if (bytes >= GB) return $"{bytes / GB:0.##} GiB";
    if (bytes >= MB) return $"{bytes / MB:0.##} MiB";
    if (bytes >= KB) return $"{bytes / KB:0.##} KiB";
    return $"{bytes} B";
}

static string Preview(string? s) =>
    s is null ? "null" : (s.Length <= 32 ? s : s[..29] + "...");


// Oben bei den globals:
static class IndexCache {
    static Dictionary<int,long>? _dict;
    static long _len;
    static DateTime _mtime;

    public static Dictionary<int,long> Get(string indexFile) {
        if (!File.Exists(indexFile)) return _dict ??= new();
        var fi = new FileInfo(indexFile);
        if (_dict is null || fi.Length != _len || fi.LastWriteTimeUtc != _mtime) {
            // einmal laden
            var dict = new Dictionary<int,long>(capacity: (int)Math.Max(16, fi.Length / 12));
            using var fs = new FileStream(indexFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 1<<20);
            using var br = new BinaryReader(fs);
            while (fs.Position < fs.Length) {
                int k = br.ReadInt32();
                long off = br.ReadInt64(); // ggf. auf Int32-Offsets umstellen
                dict[k] = off;
            }
            _dict = dict; _len = fi.Length; _mtime = fi.LastWriteTimeUtc;
        }
        return _dict!;
    }

    public static void Add(int key, long offset) {
        _dict?.Remove(key);
        _dict?.Add(key, offset);
    }
}
