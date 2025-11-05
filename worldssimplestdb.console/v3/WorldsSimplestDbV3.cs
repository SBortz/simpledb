using System.Text;

namespace worldssimplestdb.v3;

/// <summary>
/// Version 3: High-Performance Implementation mit In-Memory Index
/// 
/// Speicherformat: Identisch zu V2 - Binäres Format mit Length-Prefixing
/// ┌─────────┬─────────┬─────────┬─────────┐
/// │ keyLen  │ keyData │valueLen │valueData│
/// │ (4B)    │ (N B)   │ (4B)    │ (M B)   │
/// └─────────┴─────────┴─────────┴─────────┘
/// 
/// Eigenschaften:
/// - Write: O(1) - Anhängen + Index-Update im Speicher
/// - Read: O(1) - Direkter Seek zur Position via Index (kein Scan!)
/// - Speicher: Binärformat + In-Memory Index (Dictionary&lt;string, long&gt;)
/// 
/// Unterschiede zu V2:
/// + IndexStore hält eine Map von Key -> Datei-Offset im RAM
/// + GetAsync macht direkten Seek zur Position (keine vollständige Datei-Durchsuchung)
/// + Dramatisch schneller bei vielen Einträgen (O(1) statt O(n))
/// + Index muss beim Start einmalig aufgebaut werden (durch Scan der Datei)
/// 
/// Vorteile:
/// + Extrem schnelle Lesezugriffe auch bei Millionen von Einträgen
/// + Schreibzugriffe bleiben schnell (nur append + RAM-Update)
/// + Skaliert sehr gut
/// 
/// Nachteile:
/// - Index benötigt RAM (ca. 50-100 Bytes pro Key)
/// - Startup-Zeit zum Index-Aufbau (einmalig, aber kann bei großen DBs dauern)
/// - Komplexere Architektur mit Dependency Injection (IIndexStore)
/// 
/// Use Case: Produktive Anwendungen mit vielen Einträgen (&gt;10.000), 
///           wenn schnelle Lesezugriffe erforderlich sind
/// </summary>
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