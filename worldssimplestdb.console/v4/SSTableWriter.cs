using System.Text;

namespace worldssimplestdb.v4;

/// <summary>
/// Schreibt eine Memtable als immutable SSTable-Datei auf Disk
///     
/// Dateiformat:
/// 
/// Header (24 Bytes):
/// ┌─────────────┬─────────┬─────────────┬─────────────┬─────────────┐
/// │ MagicNumber │ Version │ EntryCount  │IndexEntryCnt│ IndexOffset │
/// │   (4B)      │  (4B)   │    (4B)     │    (4B)     │    (8B)     │
/// └─────────────┴─────────┴─────────────┴─────────────┴─────────────┘
/// 
/// Data Section (variable):
/// ┌─────────┬─────────┬─────────┬─────────┐ (wiederholt für jeden Entry)
/// │ KeyLen  │ KeyData │ValueLen │ValueData│
/// │  (4B)   │  (N B)  │  (4B)   │  (M B)  │
/// └─────────┴─────────┴─────────┴─────────┘
/// 
/// Index Section (am Ende, Sparse-Index):
/// ┌─────────┬─────────┬─────────┐ (wiederholt für jeden N-ten Entry)
/// │ KeyLen  │ KeyData │ Offset  │
/// │  (4B)   │  (N B)  │  (8B)   │
/// └─────────┴─────────┴─────────┘
/// 
/// Sparse-Index: Nur jeder N-te Key (z.B. jeder 16.) wird im Index gespeichert.
/// Der Index ermöglicht binäre Suche, da Keys sortiert sind!
/// Nach der binären Suche im Index wird linear im Bereich gescannt.
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
            FileStream? fs = null;
            BinaryWriter? bw = null;
            try
            {
                fs = new FileStream(tempFilename, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 64 * 1024);
                bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: true);
                
                var entries = memtable.GetSortedEntries().ToList();
                
                // 1. Schreibe Header (Platzhalter, wird später aktualisiert)
                long headerPos = fs.Position;
                bw.Write(SSTableFormat.MagicNumber);
                bw.Write(SSTableFormat.Version);
                bw.Write(entries.Count);
                bw.Write(0); // IndexEntryCount Platzhalter
                bw.Write(0L); // IndexOffset Platzhalter
                
                // 2. Schreibe sortierte Data Section und sammle Sparse-Index-Einträge
                var indexEntries = new List<(string key, long offset)>();
                int entryIndex = 0;
                
                foreach (var entry in entries)
                {
                    long offset = fs.Position;
                    
                    // Nur jeden N-ten Key in den Index aufnehmen (Sparse-Index)
                    if (entryIndex % SSTableFormat.SparseIndexDensity == 0)
                    {
                        indexEntries.Add((entry.Key, offset));
                    }
                    
                    byte[] keyBytes = Encoding.UTF8.GetBytes(entry.Key);
                    byte[] valueBytes = Encoding.UTF8.GetBytes(entry.Value);
                    
                    bw.Write(keyBytes.Length);
                    bw.Write(keyBytes);
                    bw.Write(valueBytes.Length);
                    bw.Write(valueBytes);
                    
                    entryIndex++;
                }
                
                // 3. Schreibe Sparse-Index Section
                long indexOffset = fs.Position;
                
                foreach (var (key, offset) in indexEntries)
                {
                    byte[] keyBytes = Encoding.UTF8.GetBytes(key);
                    bw.Write(keyBytes.Length);
                    bw.Write(keyBytes);
                    bw.Write(offset);
                }
            
                // 4. Aktualisiere Header mit korrektem IndexEntryCount und IndexOffset
                fs.Seek(headerPos + 12, SeekOrigin.Begin); // Position von IndexEntryCount im Header
                bw.Write(indexEntries.Count);
                bw.Write(indexOffset);
                
                // WICHTIG: Flush alles auf Disk bevor Dispose (für Crash-Safety)
                // Flush BinaryWriter zuerst (schreibt gepufferte Daten in FileStream)
                bw.Flush();
                
                // Dispose BinaryWriter (mit leaveOpen: true bleibt fs offen)
                bw.Dispose();
                bw = null;
                
                // Dann Flush FileStream (schreibt auf Disk)
                await fs.FlushAsync();
                
                // Dispose FileStream (schließt die Datei)
                await fs.DisposeAsync();
                fs = null;
            }
            finally
            {
                // Cleanup falls etwas schief geht
                bw?.Dispose();
                fs?.Dispose();
            }
            
            // Datei ist jetzt geschlossen und alles ist auf Disk
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

