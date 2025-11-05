using System.Text;

namespace worldssimplestdb.v4;

/// <summary>
/// Schreibt eine Memtable als immutable SSTable-Datei auf Disk
/// </summary>
public class SSTableWriter
{
    /// <summary>
    /// Schreibt die Memtable als neue SSTable-Datei
    /// </summary>
    /// <returns>Den Pfad zur erstellten SSTable-Datei</returns>
    public static async Task<string> FlushAsync(Memtable memtable, string directory)
    {
        if (!Directory.Exists(directory))
            Directory.CreateDirectory(directory);
        
        // Generiere eindeutigen Dateinamen mit Timestamp
        string timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
        string filename = Path.Combine(directory, $"sstable_{timestamp}.sst");
        
        await using var fs = new FileStream(filename, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 64 * 1024);
        await using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
        
        var entries = memtable.GetSortedEntries().ToList();
        
        // 1. Schreibe Header (Platzhalter, wird später aktualisiert)
        long headerPos = fs.Position;
        bw.Write(SSTableFormat.MagicNumber);
        bw.Write(SSTableFormat.Version);
        bw.Write(entries.Count);
        bw.Write(0L); // IndexOffset Platzhalter
        
        // 2. Schreibe sortierte Data Section und sammle Index-Einträge
        var indexEntries = new List<(string key, long offset)>();
        
        foreach (var entry in entries)
        {
            long offset = fs.Position;
            indexEntries.Add((entry.Key, offset));
            
            byte[] keyBytes = Encoding.UTF8.GetBytes(entry.Key);
            byte[] valueBytes = Encoding.UTF8.GetBytes(entry.Value);
            
            bw.Write(keyBytes.Length);
            bw.Write(keyBytes);
            bw.Write(valueBytes.Length);
            bw.Write(valueBytes);
        }
        
        // 3. Schreibe Index Section
        long indexOffset = fs.Position;
        
        foreach (var (key, offset) in indexEntries)
        {
            byte[] keyBytes = Encoding.UTF8.GetBytes(key);
            bw.Write(keyBytes.Length);
            bw.Write(keyBytes);
            bw.Write(offset);
        }
        
        // 4. Aktualisiere Header mit korrektem IndexOffset
        fs.Seek(headerPos + 12, SeekOrigin.Begin); // Position von IndexOffset im Header
        bw.Write(indexOffset);
        
        await fs.FlushAsync();
        
        return filename;
    }
}

