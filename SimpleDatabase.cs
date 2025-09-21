using System.Diagnostics;
using System.Text;

namespace MicroDb;

public class SimpleDatabase(IIndexCache indexCache, string dataFile = "database.bin")
{
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

        indexCache.WriteEntry(key, offset);
    }

    public string? GetIndexed(string searchKey)
    {
        var index = indexCache.Get("database.idx");

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
}