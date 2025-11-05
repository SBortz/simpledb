using System.Text;

namespace worldssimplestdb.v4;

/// <summary>
/// Schreibt eine Memtable als immutable SSTable-Datei auf Disk
///     
/// Dateiformat:
/// 
/// Header (16 Bytes):
/// ┌─────────────┬─────────┬─────────────┬─────────────┐
/// │ MagicNumber │ Version │ EntryCount  │ IndexOffset │
/// │   (4B)      │  (4B)   │    (4B)     │    (8B)     │
/// └─────────────┴─────────┴─────────────┴─────────────┘
/// 
/// Data Section (variable):
/// ┌─────────┬─────────┬─────────┬─────────┐ (wiederholt für jeden Entry)
/// │ KeyLen  │ KeyData │ValueLen │ValueData│
/// │  (4B)   │  (N B)  │  (4B)   │  (M B)  │
/// └─────────┴─────────┴─────────┴─────────┘
/// 
/// Index Section (am Ende):
/// ┌─────────┬─────────┬─────────┐ (wiederholt für jeden Entry)
/// │ KeyLen  │ KeyData │ Offset  │
/// │  (4B)   │  (N B)  │  (8B)   │
/// └─────────┴─────────┴─────────┘
/// 
/// Der Index ermöglicht binäre Suche, da Keys sortiert sind!
/// </summary>
public class SSTableWriter
{
    /// <summary>
    /// Schreibt die Memtable als neue SSTable-Datei (atomar)
    /// 
    /// Verwendet Temp-File + Rename für Atomizität:
    /// - Schreibt zuerst in .tmp Datei
    /// - Flush komplett
    /// - Rename zu .sst (atomic auf meisten Filesystemen)
    /// 
    /// Verhindert korrupte SSTable-Dateien bei Crash während Flush!
    /// </summary>
    /// <returns>Den Pfad zur erstellten SSTable-Datei</returns>
    public static async Task<string> FlushAsync(Memtable memtable, string directory)
    {
        if (!Directory.Exists(directory))
            Directory.CreateDirectory(directory);
        
        // Generiere eindeutigen Dateinamen mit Timestamp
        string timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
        string tempFilename = Path.Combine(directory, $"sstable_{timestamp}.tmp");
        string finalFilename = Path.Combine(directory, $"sstable_{timestamp}.sst");
        
        try
        {
            // Schreibe zuerst in Temp-Datei (für Atomizität)
            await using var fs = new FileStream(tempFilename, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 64 * 1024);
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
            
            // WICHTIG: Flush alles auf Disk bevor Rename (für Crash-Safety)
            await fs.FlushAsync();
            
            // Schließe Datei explizit (garantiert dass alles auf Disk ist)
            bw.Close();
            fs.Close();
            
            // Atomarer Rename: Temp → Final (auf meisten Filesystemen atomic)
            // Falls Crash während Rename: Temp-Datei bleibt, kann gelöscht werden
            File.Move(tempFilename, finalFilename, overwrite: false);
            
            return finalFilename;
        }
        catch (Exception ex)
        {
            // Cleanup: Lösche Temp-Datei bei Fehler
            if (File.Exists(tempFilename))
            {
                try
                {
                    File.Delete(tempFilename);
                }
                catch
                {
                    // Ignoriere Cleanup-Fehler
                }
            }
            throw new InvalidOperationException($"Failed to write SSTable: {ex.Message}", ex);
        }
    }
}

