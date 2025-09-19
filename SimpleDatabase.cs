using System.Diagnostics;
using System.Text;

namespace MicroDb;

public class SimpleDatabase(string dataFile = "database.bin", string indexFile = "database.idx")
    : IDisposable
{
    private bool _disposed;

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
        IndexCache.Add(key, offset);   // <— Cache aktuell halten
    }

    public string? GetIndexed(string searchKey)
    {
        var index = IndexCache.Get(indexFile);

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

    public (long entries, string lastKey, long dataBytes) Fill(long targetBytes, int minLen, int maxLen)
    {
        using var fs = new FileStream(dataFile, FileMode.Append, FileAccess.Write, FileShare.Read, 1 << 20, FileOptions.SequentialScan);
        using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
        using var fi = new FileStream(indexFile, FileMode.Append, FileAccess.Write, FileShare.Read, 1 << 20, FileOptions.SequentialScan);
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
        if (File.Exists(dataFile)) File.Delete(dataFile);
        if (File.Exists(indexFile)) File.Delete(indexFile);
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