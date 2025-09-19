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