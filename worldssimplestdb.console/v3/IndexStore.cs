using System.Text;

namespace worldssimplestdb.v3;

public class IndexStore(string indexFile = "database.idx") : IIndexStore
{
    public Dictionary<string, long> Get()
    {
        return GetWithFeedback();
    }

    public Dictionary<string, long> GetWithFeedback(Action<string>? feedback = null)
    {
        if (!File.Exists(indexFile)) return new();
        
        feedback?.Invoke($"Loading index from file...");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        var fi = new FileInfo(indexFile);
        var dict = new Dictionary<string, long>(capacity: (int)Math.Max(16, fi.Length / 20));
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
        feedback?.Invoke($"Index loaded: {entries:N0} entries in {sw.ElapsedMilliseconds}ms");
        return dict;
    }

    public bool TryGetValue(string key, out long offset)
    {
        var dict = Get();
        return dict.TryGetValue(key, out offset);
    }


    public void WriteEntry(string key, long offset)
    {
        byte[] keyData = Encoding.UTF8.GetBytes(key);
        
        using var fi = new FileStream(indexFile, FileMode.Append, FileAccess.Write, FileShare.Read);
        using var iw = new BinaryWriter(fi);
        iw.Write(keyData.Length);
        iw.Write(keyData);
        iw.Write(offset);
        
    }
}