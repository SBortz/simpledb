using System.Text;

namespace worldssimplestdb.v1;

public class WorldsSimplestDbV1(string dataFile = "database.txt") : IDatabase
{
    private readonly SemaphoreSlim writeSemaphore = new(1, 1);
    private bool disposed = false;
    
    public async Task SetAsync(string key, string value)
    {
        await writeSemaphore.WaitAsync();
        try
        {
            string line = $"{key};{value}{Environment.NewLine}";
            await File.AppendAllTextAsync(dataFile, line, Encoding.UTF8);
        }
        finally
        {
            writeSemaphore.Release();
        }
    }

    public async Task<string?> GetAsync(string searchKey)
    {
        if (!File.Exists(dataFile))
            return null;
            
        string? last = null;
        string[] lines = await File.ReadAllLinesAsync(dataFile, Encoding.UTF8);
        
        foreach (string line in lines)
        {
            string[] parts = line.Split(';', 2);
            if (parts.Length != 2) continue;
            string key = parts[0];
            string value = parts[1];
                
            if (key == searchKey)
                last = value;
        }
        
        return last;
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