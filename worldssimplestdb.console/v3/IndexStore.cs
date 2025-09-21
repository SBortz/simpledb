using System.Text;

namespace worldssimplestdb.v3;

public class IndexStore(string indexFile = "database.idx") : IIndexStore
{
    private Dictionary<string, long> indexDict = new();

    public void Load(Action<string>? feedback = null)
    {
        if (!File.Exists(indexFile)) 
        {
            return;
        }
        
        feedback?.Invoke($"Loading index from file...");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        var fi = new FileInfo(indexFile);
        indexDict = new Dictionary<string, long>(capacity: (int)Math.Max(16, fi.Length / 20));
        using var fs = new FileStream(indexFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 1 << 20);
        using var br = new BinaryReader(fs);
        
        long entries = 0;
        while (fs.Position < fs.Length)
        {
            int keyLen = br.ReadInt32();
            var keyBuf = br.ReadBytes(keyLen);
            long off = br.ReadInt64();
            string key = Encoding.UTF8.GetString(keyBuf);
            indexDict[key] = off;
            entries++;
            
            // Progress feedback alle 10000 Einträge
            if (entries % 10000 == 0)
            {
                feedback?.Invoke($"Loading index... {entries:N0} entries loaded");
            }
        }
        
        sw.Stop();
        feedback?.Invoke($"Index loaded: {entries:N0} entries in {sw.ElapsedMilliseconds}ms");
    }

    public bool TryGetValue(string key, out long offset)
    {
        return this.indexDict.TryGetValue(key, out offset);
    }

    public void WriteEntry(string key, long offset)
    {
        // Update in-memory index
        indexDict[key] = offset;
        
        // Write to index file
        byte[] keyData = Encoding.UTF8.GetBytes(key);
        
        using var fi = new FileStream(indexFile, FileMode.Append, FileAccess.Write, FileShare.Read);
        using var iw = new BinaryWriter(fi);
        iw.Write(keyData.Length);
        iw.Write(keyData);
        iw.Write(offset);
    }
}