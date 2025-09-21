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
            // Escape semicolons in key and value
            string escapedKey = key.Replace(";", ";;");
            string escapedValue = value.Replace(";", ";;");
            
            // Write in human-readable format: key;value
            string line = $"{escapedKey};{escapedValue}{Environment.NewLine}";
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
        var lines = await File.ReadAllLinesAsync(dataFile, Encoding.UTF8);
        
        foreach (var line in lines)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
                
            var parts = SplitLine(line);
            if (parts.Length == 2)
            {
                string key = parts[0].Replace(";;", ";");
                string value = parts[1].Replace(";;", ";");
                
                if (key == searchKey)
                    last = value;
            }
        }
        
        return last;
    }
    
    private string[] SplitLine(string line)
    {
        var parts = new List<string>();
        var current = new StringBuilder();
        
        for (int i = 0; i < line.Length; i++)
        {
            if (line[i] == ';' && i + 1 < line.Length && line[i + 1] == ';')
            {
                // Escaped semicolon
                current.Append(';');
                i++; // Skip the next semicolon
            }
            else if (line[i] == ';')
            {
                // Separator
                parts.Add(current.ToString());
                current.Clear();
            }
            else
            {
                current.Append(line[i]);
            }
        }
        
        if (current.Length > 0)
            parts.Add(current.ToString());
            
        return parts.ToArray();
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