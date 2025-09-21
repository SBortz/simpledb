using System.Text;

namespace worldssimplestdb.v3;

public class IndexStore(string indexFile = "database.idx") : IIndexStore
{
    private Dictionary<string, long>? dict;
    private long len;
    private DateTime _mtime;

    public Dictionary<string, long> Get()
    {
        if (!File.Exists(indexFile)) return dict ??= new();
        
        // If we already have a loaded cache, return it immediately
        // This prevents reloading when we know the cache is up-to-date
        if (dict != null)
        {
            return dict;
        }
        
        // Only reload if cache is null (first time)
        return LoadIndex(indexFile);
    }

    private Dictionary<string, long> LoadIndex(string indexFile)
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
        this.dict = dict; len = fi.Length; _mtime = fi.LastWriteTimeUtc;
        return this.dict;
    }

    public Dictionary<string, long> GetWithFeedback(Action<string>? feedback = null)
    {
        if (!File.Exists(indexFile)) return dict ??= new();
        var fi = new FileInfo(indexFile);
        if (dict is null || fi.Length != len || fi.LastWriteTimeUtc != _mtime)
        {
            feedback?.Invoke($"Loading index from file...");
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
            this.dict = dict; len = fi.Length; _mtime = fi.LastWriteTimeUtc;
            feedback?.Invoke($"Index loaded: {entries:N0} entries in {sw.ElapsedMilliseconds}ms");
        }
        return dict!;
    }

    public bool TryGetValue(string key, out long offset)
    {
        // Ensure cache is loaded
        Get();
        return dict!.TryGetValue(key, out offset);
    }

    public void Add(string key, long offset)
    {
        if (dict != null)
        {
            dict.Remove(key);
            dict.Add(key, offset);
        }
    }

    public void AddAndUpdateMetadata(string key, long offset)
    {
        // Ensure dict is initialized
        if (dict == null)
        {
            dict = new Dictionary<string, long>();
        }
        
        dict.Remove(key);
        dict.Add(key, offset);
        
        // Update metadata to prevent full reload
        var fi = new FileInfo(indexFile);
        if (fi.Exists)
        {
            len = fi.Length;
            _mtime = fi.LastWriteTimeUtc;
        }
    }

    public void UpdateMetadata(long length, DateTime lastWriteTime)
    {
        len = length;
        _mtime = lastWriteTime;
    }

    public void WriteEntry(string key, long offset)
    {
        byte[] keyData = Encoding.UTF8.GetBytes(key);
        
        using var fi = new FileStream(indexFile, FileMode.Append, FileAccess.Write, FileShare.Read);
        using var iw = new BinaryWriter(fi);
        iw.Write(keyData.Length);
        iw.Write(keyData);
        iw.Write(offset);
        
        // Update cache
        AddAndUpdateMetadata(key, offset);
    }

    public void Clear()
    {
        dict = null;
        len = 0;
        _mtime = default;
    }
}