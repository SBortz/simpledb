using System.Text;

namespace MicroDb;

public static class IndexCache
{
    private static Dictionary<string, long>? _dict;
    private static long _len;
    private static DateTime _mtime;

    public static Dictionary<string, long> Get(string indexFile)
    {
        if (!File.Exists(indexFile)) return _dict ??= new();
        
        // If we already have a loaded cache, return it immediately
        // This prevents reloading when we know the cache is up-to-date
        if (_dict != null)
        {
            return _dict;
        }
        
        // Only reload if cache is null (first time)
        return LoadIndex(indexFile);
    }

    private static Dictionary<string, long> LoadIndex(string indexFile)
    {
        var fi = new FileInfo(indexFile);
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
        return _dict;
    }

    public static Dictionary<string, long> GetWithFeedback(string indexFile, Action<string>? feedback = null)
    {
        if (!File.Exists(indexFile)) return _dict ??= new();
        var fi = new FileInfo(indexFile);
        if (_dict is null || fi.Length != _len || fi.LastWriteTimeUtc != _mtime)
        {
            feedback?.Invoke($"Loading index from {Utils.FormatSize(fi.Length)} file...");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            
            // einmal laden
            var dict = new Dictionary<string, long>(capacity: (int)Math.Max(16, fi.Length / 20)); // Schätzung für String Keys
            using var fs = new FileStream(indexFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 1 << 20);
            using var br = new BinaryReader(fs);
            
            long entries = 0;
            while (fs.Position < fs.Length)
            {
                int keyLen = br.ReadInt32();
                var keyBuf = br.ReadBytes(keyLen);
                long off = br.ReadInt64();
                string key = Encoding.UTF8.GetString(keyBuf);
                dict[key] = off;
                entries++;
                
                // Progress feedback alle 10000 Einträge
                if (entries % 10000 == 0)
                {
                    feedback?.Invoke($"Loading index... {entries:N0} entries loaded");
                }
            }
            
            sw.Stop();
            _dict = dict; _len = fi.Length; _mtime = fi.LastWriteTimeUtc;
            feedback?.Invoke($"Index loaded: {entries:N0} entries in {sw.ElapsedMilliseconds}ms");
        }
        return _dict!;
    }

    public static void Add(string key, long offset)
    {
        if (_dict != null)
        {
            _dict.Remove(key);
            _dict.Add(key, offset);
        }
    }

    public static void AddAndUpdateMetadata(string key, long offset, string indexFile)
    {
        // Ensure dict is initialized
        if (_dict == null)
        {
            _dict = new Dictionary<string, long>();
        }
        
        _dict.Remove(key);
        _dict.Add(key, offset);
        
        // Update metadata to prevent full reload
        var fi = new FileInfo(indexFile);
        if (fi.Exists)
        {
            _len = fi.Length;
            _mtime = fi.LastWriteTimeUtc;
        }
    }

    public static void UpdateMetadata(long length, DateTime lastWriteTime)
    {
        _len = length;
        _mtime = lastWriteTime;
    }

    public static void Clear()
    {
        _dict = null;
        _len = 0;
        _mtime = default;
    }
}