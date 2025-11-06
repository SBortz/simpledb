using System.Text;

namespace worldssimplestdb.v2;

/// <summary>
/// Version 2: Optimierte Implementation mit Binär-Format
/// 
/// Speicherformat: Binäres Format mit Length-Prefixing
/// ┌─────────┬─────────┬─────────┬─────────┐
/// │ keyLen  │ keyData │valueLen │valueData│
/// │ (4B)    │ (N B)   │ (4B)    │ (M B)   │
/// └─────────┴─────────┴─────────┴─────────┘
/// 
/// Eigenschaften:
/// - Write: O(1) - Neue Einträge werden binär an die Datei angehängt
/// - Read: O(n) - Die gesamte Datei wird sequenziell durchsucht
/// - Speicher: Binärformat, kompakter als V1
/// 
/// Unterschiede zu V1:
/// + Binärformat ist effizienter als Text (kein Parsing, kein Encoding-Overhead)
/// + Kann beliebige Strings speichern (auch mit Semikolon oder Newlines)
/// + Etwas schneller beim Lesen durch strukturiertes Format
/// - Datei nicht mehr human-readable
/// 
/// Nachteile:
/// - Immer noch O(n) Lesezugriffe (vollständiger Scan)
/// - Keine Index-Struktur für schnelle Lookups
/// 
/// Use Case: Kleine bis mittlere Datenmengen (1.000-10.000 Einträge), 
///           wenn binäres Format bevorzugt wird
/// </summary>
public class WorldsSimplestDbV2(string? dataFile = null) : IDatabase
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
            await using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
            bw.Write(keyData.Length);
            bw.Write(keyData);
            bw.Write(valueData.Length);
            bw.Write(valueData);
            await fs.FlushAsync();
        }
        finally
        {
            writeSemaphore.Release();
        }
    }

    public async Task<string?> GetAsync(string searchKey)
    {
        if (!File.Exists(_dataFile))
            return null;
        
        // Lade die gesamte Datei auf einmal (effizienter als viele kleine Reads)
        byte[] fileData = await File.ReadAllBytesAsync(_dataFile);
        
        string? last = null;
        int position = 0;
        
        while (position < fileData.Length)
        {
            // Lese keyLen
            if (position + 4 > fileData.Length) break;
            int keyLen = BitConverter.ToInt32(fileData, position);
            position += 4;
            
            // Lese keyData
            if (position + keyLen > fileData.Length) break;
            byte[] keyBuf = new byte[keyLen];
            Array.Copy(fileData, position, keyBuf, 0, keyLen);
            position += keyLen;
            
            // Lese valueLen
            if (position + 4 > fileData.Length) break;
            int valueLen = BitConverter.ToInt32(fileData, position);
            position += 4;
            
            // Lese valueData
            if (position + valueLen > fileData.Length) break;
            byte[] valueBuf = new byte[valueLen];
            Array.Copy(fileData, position, valueBuf, 0, valueLen);
            position += valueLen;
            
            string key = Encoding.UTF8.GetString(keyBuf);
            if (key == searchKey)
                last = Encoding.UTF8.GetString(valueBuf);
        }
        
        return last;
    }

    public void Dispose()
    {
        if (disposed) return;
        writeSemaphore?.Dispose();
        disposed = true;
    }
}