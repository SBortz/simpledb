using System.Diagnostics;
using System.Text;

namespace MicroDb;

public class SimpleDatabase(IIndexCache indexCache, string dataFile = "database.bin") : IDisposable
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

    public async Task<string?> GetIndexedAsync(string searchKey)
    {
        var index = indexCache.Get("database.idx");

        if (!index.TryGetValue(searchKey, out var pos))
            return null;

        using var fs = new FileStream(dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 4096, useAsync: true);
        using var br = new BinaryReader(fs);
        fs.Seek(pos, SeekOrigin.Begin);

        int keyLen = br.ReadInt32();
        var keyBuf = br.ReadBytes(keyLen);
        int valueLen = br.ReadInt32();
        var valueBuf = br.ReadBytes(valueLen);
        return Encoding.UTF8.GetString(valueBuf);
    }

    public async Task<string?> GetScanAsync(string searchKey)
    {
        string? last = null;
        using var fs = new FileStream(dataFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, bufferSize: 4096, useAsync: true);
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

    public async Task ClearAsync()
    {
        if (File.Exists(dataFile)) File.Delete(dataFile);
        if (File.Exists("database.idx")) File.Delete("database.idx");
        indexCache.Clear();
        await Task.CompletedTask; // Make it async
    }

    public void Dispose()
    {
        if (!disposed)
        {
            writeSemaphore?.Dispose();
            disposed = true;
        }
    }
}