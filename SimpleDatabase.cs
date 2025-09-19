using System.Diagnostics;
using System.Text;

namespace MicroDb;

public class SimpleDatabase : IDisposable
{
    private readonly string _dataFile;
    private readonly string _indexFile;
    private bool _disposed = false;

    public SimpleDatabase(string dataFile = "database.bin", string indexFile = "database.idx")
    {
        _dataFile = dataFile;
        _indexFile = indexFile;
    }

    public void Set(string key, string value)
    {
        byte[] keyData = Encoding.UTF8.GetBytes(key);
        byte[] valueData = Encoding.UTF8.GetBytes(value);

        using var fs = new FileStream(_dataFile, FileMode.Append, FileAccess.Write, FileShare.Read);
        long offset = fs.Position;

        using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
        bw.Write(keyData.Length);
        bw.Write(keyData);
        bw.Write(valueData.Length);
        bw.Write(valueData);
        bw.Flush();

        using var fi = new FileStream(_indexFile, FileMode.Append, FileAccess.Write, FileShare.Read);
        using var iw = new BinaryWriter(fi);
        iw.Write(keyData.Length);
        iw.Write(keyData);
        iw.Write(offset);
        IndexCache.Add(key, offset);   // <— Cache aktuell halten
    }

    public string? GetIndexed(string searchKey)
    {
        var index = IndexCache.Get(_indexFile);

        if (!index.TryGetValue(searchKey, out var pos))
            return null;

        using var fs = new FileStream(_dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var br = new BinaryReader(fs);
        fs.Seek(pos, SeekOrigin.Begin);

        int keyLen = br.ReadInt32();
        var keyBuf = br.ReadBytes(keyLen);
        int valueLen = br.ReadInt32();
        var valueBuf = br.ReadBytes(valueLen);
        return Encoding.UTF8.GetString(valueBuf);
    }

    public string? GetScan(string searchKey)
    {
        string? last = null;
        using var fs = new FileStream(_dataFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
        using var br = new BinaryReader(fs);

        while (fs.Position < fs.Length)
        {
            int keyLen = br.ReadInt32();
            var keyBuf = br.ReadBytes(keyLen);
            int valueLen = br.ReadInt32();
            var valueBuf = br.ReadBytes(valueLen);
            
            string key = Encoding.UTF8.GetString(keyBuf);
            if (key == searchKey)
                last = Encoding.UTF8.GetString(valueBuf);
        }
        return last;
    }

    public (long entries, string lastKey, long dataBytes) Fill(long targetBytes, int minLen, int maxLen)
    {
        using var fs = new FileStream(_dataFile, FileMode.Append, FileAccess.Write, FileShare.Read, 1 << 20, FileOptions.SequentialScan);
        using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
        using var fi = new FileStream(_indexFile, FileMode.Append, FileAccess.Write, FileShare.Read, 1 << 20, FileOptions.SequentialScan);
        using var iw = new BinaryWriter(fi, Encoding.UTF8, leaveOpen: true);

        var rnd = Random.Shared;
        var sb = new StringBuilder(maxLen);
        int keyNum = 1;
        long entries = 0;

        while (fs.Length < targetBytes)
        {
            int len = rnd.Next(minLen, maxLen + 1);
            sb.Clear();
            sb.Append('v').Append(keyNum).Append('-');
            while (sb.Length < len) sb.Append('x');
            
            string key = $"key{keyNum}";
            string value = sb.ToString();
            byte[] keyData = Encoding.UTF8.GetBytes(key);
            byte[] valueData = Encoding.UTF8.GetBytes(value);

            long off = fs.Position;
            bw.Write(keyData.Length);
            bw.Write(keyData);
            bw.Write(valueData.Length);
            bw.Write(valueData);

            iw.Write(keyData.Length);
            iw.Write(keyData);
            iw.Write(off);

            entries++;
            if ((keyNum & 0x3FFF) == 0) { bw.Flush(); iw.Flush(); }

            keyNum++;
        }

        bw.Flush(); iw.Flush();
        return (entries, $"key{keyNum - 1}", fs.Length);
    }

    public void Clear()
    {
        if (File.Exists(_dataFile)) File.Delete(_dataFile);
        if (File.Exists(_indexFile)) File.Delete(_indexFile);
        IndexCache.Clear();
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            // Cleanup falls nötig
            _disposed = true;
        }
    }
}

public static class IndexCache
{
    private static Dictionary<string, long>? _dict;
    private static long _len;
    private static DateTime _mtime;

    public static Dictionary<string, long> Get(string indexFile)
    {
        if (!File.Exists(indexFile)) return _dict ??= new();
        var fi = new FileInfo(indexFile);
        if (_dict is null || fi.Length != _len || fi.LastWriteTimeUtc != _mtime)
        {
            // einmal laden
            var dict = new Dictionary<string, long>(capacity: (int)Math.Max(16, fi.Length / 20)); // Schätzung für String Keys
            using var fs = new FileStream(indexFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 1 << 20);
            using var br = new BinaryReader(fs);
            while (fs.Position < fs.Length)
            {
                int keyLen = br.ReadInt32();
                var keyBuf = br.ReadBytes(keyLen);
                long off = br.ReadInt64();
                string key = Encoding.UTF8.GetString(keyBuf);
                dict[key] = off;
            }
            _dict = dict; _len = fi.Length; _mtime = fi.LastWriteTimeUtc;
        }
        return _dict!;
    }

    public static void Add(string key, long offset)
    {
        _dict?.Remove(key);
        _dict?.Add(key, offset);
    }

    public static void Clear()
    {
        _dict = null;
        _len = 0;
        _mtime = default;
    }
}

public static class Utils
{
    public static void Require(bool cond, string usage)
    {
        if (!cond) throw new ArgumentException("Usage: " + usage);
    }

    public static long ParseSize(string s)
    {
        s = s.Trim().ToLowerInvariant();
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