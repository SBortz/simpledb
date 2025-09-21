using System.Text;

namespace worldssimplestdb.v3;

public class WorldsSimplestDbV3(IIndexCache indexCache, string dataFile = "database.bin") : IDatabase
{
    private readonly SemaphoreSlim writeSemaphore = new(1, 1);
    private bool disposed = false;
    
    public async Task SetAsync(string key, string value)
    {
        await writeSemaphore.WaitAsync();
        try
        {
            byte[] keyData = Encoding.UTF8.GetBytes(key);
            byte[] valueData = Encoding.UTF8.GetBytes(value);

            await using var fs = new FileStream(dataFile, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 4096, useAsync: true);
            long offset = fs.Position;

            await using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
            bw.Write(keyData.Length);
            bw.Write(keyData);
            bw.Write(valueData.Length);
            bw.Write(valueData);
            await fs.FlushAsync();

            indexCache.WriteEntry(key, offset);
        }
        finally
        {
            writeSemaphore.Release();
        }
    }

    public async Task<string?> GetAsync(string searchKey)
    {
        if (!indexCache.TryGetValue(searchKey, "database.idx", out long pos))
            return null;

        await using var fs = new FileStream(dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 4096, useAsync: true);
        using var br = new BinaryReader(fs);
        fs.Seek(pos, SeekOrigin.Begin);

        // Skip key data (keyLen + keyData)
        int keyLen = br.ReadInt32();
        fs.Seek(keyLen, SeekOrigin.Current);
        
        // Read value data
        int valueLen = br.ReadInt32();
        byte[] valueBuf = br.ReadBytes(valueLen);
        
        return Encoding.UTF8.GetString(valueBuf);
    }


    public void Dispose()
    {
        if (disposed) return;
        writeSemaphore?.Dispose();
        disposed = true;
    }
}