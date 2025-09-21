using System.Text;

namespace worldssimplestdb.v3;

// Data format
// ┌─────────┬─────────┬─────────┬─────────┐
// │ keyLen  │ keyData │valueLen │valueData│
// │ (4B)    │ (N B)   │ (4B)    │ (M B)   │
// └─────────┴─────────┴─────────┴─────────┘

public class WorldsSimplestDbV3(IIndexStore indexStore, string? dataFile = null) : IDatabase
{
    private readonly string _dataFile = dataFile ?? GetSolutionDatabasePath("database.bin");
    private readonly SemaphoreSlim writeSemaphore = new(1, 1);
    private bool disposed = false;
    
    private static string GetSolutionDatabasePath(string fileName)
    {
        // Find the solution directory by looking for .sln file
        var currentDir = Directory.GetCurrentDirectory();
        var dir = new DirectoryInfo(currentDir);
        
        while (dir != null && !dir.GetFiles("*.sln").Any())
        {
            dir = dir.Parent;
        }
        
        return dir != null ? Path.Combine(dir.FullName, fileName) : fileName;
    }
    
    public async Task SetAsync(string key, string value)
    {
        await writeSemaphore.WaitAsync();
        try
        {
            byte[] keyData = Encoding.UTF8.GetBytes(key);
            byte[] valueData = Encoding.UTF8.GetBytes(value);

            await using var fs = new FileStream(_dataFile, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 4096, useAsync: true);
            long offset = fs.Position;

            await using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
            bw.Write(keyData.Length);
            bw.Write(keyData);
            bw.Write(valueData.Length);
            bw.Write(valueData);
            await fs.FlushAsync();

            indexStore.WriteEntry(key, offset);
        }
        finally
        {
            writeSemaphore.Release();
        }
    }

    public async Task<string?> GetAsync(string searchKey)
    {
        if (!indexStore.TryGetValue(searchKey, out long pos))
            return null;

        await using var fs = new FileStream(_dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 4096, useAsync: true);
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