using System.Diagnostics;
using System.Text;

namespace MicroDb;

public class SimpleDatabase : IDisposable
{
    private readonly string dataFile;
    private readonly string indexFile;
    private readonly IIndexCache indexCache;
    private bool disposed;

    public SimpleDatabase(IIndexCache indexCache, string dataFile = "database.bin", string indexFile = "database.idx")
    {
        this.indexCache = indexCache;
        this.dataFile = dataFile;
        this.indexFile = indexFile;
    }


    public void Set(string key, string value)
    {
        byte[] keyData = Encoding.UTF8.GetBytes(key);
        byte[] valueData = Encoding.UTF8.GetBytes(value);

        using var fs = new FileStream(dataFile, FileMode.Append, FileAccess.Write, FileShare.Read);
        long offset = fs.Position;

        using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
        bw.Write(keyData.Length);
        bw.Write(keyData);
        bw.Write(valueData.Length);
        bw.Write(valueData);
        bw.Flush();

        using var fi = new FileStream(indexFile, FileMode.Append, FileAccess.Write, FileShare.Read);
        using var iw = new BinaryWriter(fi);
        iw.Write(keyData.Length);
        iw.Write(keyData);
        iw.Write(offset);
        indexCache.AddAndUpdateMetadata(key, offset, indexFile);   // <— Cache aktuell halten
    }

    public string? GetIndexed(string searchKey)
    {
        var index = indexCache.Get(indexFile);

        if (!index.TryGetValue(searchKey, out var pos))
            return null;

        using var fs = new FileStream(dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
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
        using var fs = new FileStream(dataFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
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


    public void Clear()
    {
        if (File.Exists(dataFile)) File.Delete(dataFile);
        if (File.Exists(indexFile)) File.Delete(indexFile);
        indexCache.Clear();
    }

    public void Dispose()
    {
        if (!disposed)
        {
            // Cleanup falls nötig
            disposed = true;
        }
    }
}