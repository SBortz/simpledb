using System.Text;

namespace worldssimplestdb.v4;

/// <summary>
/// Liest aus einer immutable SSTable-Datei mit effizientem Sparse-Index-basierten Lookup
/// 
/// Verwendet einen Sparse-Index: Nur jeder N-te Key ist im Index gespeichert.
/// Lookup-Prozess:
/// 1. Binäre Suche im Sparse-Index → findet Bereich (z.B. zwischen key_00000016 und key_00000032)
/// 2. Linearer Scan in diesem Bereich → findet exakten Key
/// </summary>
public class SSTableReader : IDisposable
{
    private readonly string _filename;
    private readonly List<IndexEntry> _index = new();
    private readonly long _indexOffset; // Offset des Index-Sections (für End-Begrenzung beim Scan)
    private bool _disposed = false;
    
    public string Filename => _filename;
    public int EntryCount => _index.Count;
    public DateTime CreationTime { get; private set; }
    
    private record IndexEntry(string Key, long Offset);
    
    public SSTableReader(string filename)
    {
        _filename = filename;
        CreationTime = File.GetCreationTimeUtc(filename);
        _indexOffset = LoadIndex();
    }
    
    /// <summary>
    /// Lädt den Index aus der SSTable-Datei in den Speicher
    /// </summary>
    /// <returns>Das Index-Offset für die End-Begrenzung beim Scan</returns>
    private long LoadIndex()
    {
        using var fs = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 64 * 1024);
        using var br = new BinaryReader(fs, Encoding.UTF8);
        
        // 1. Lese und validiere Header
        int magic = br.ReadInt32();
        if (magic != SSTableFormat.MagicNumber)
            throw new InvalidDataException($"Invalid SSTable file: {_filename}");
        
        int version = br.ReadInt32();
        if (version != SSTableFormat.Version)
            throw new InvalidDataException($"Unsupported SSTable version: {version}");
        
        int entryCount = br.ReadInt32();
        int indexEntryCount = br.ReadInt32(); // Anzahl der Index-Einträge (Sparse-Index)
        long indexOffset = br.ReadInt64();
        
        // 2. Springe zum Index und lade alle Sparse-Index-Einträge
        fs.Seek(indexOffset, SeekOrigin.Begin);
        
        for (int i = 0; i < indexEntryCount; i++)
        {
            int keyLen = br.ReadInt32();
            byte[] keyBytes = br.ReadBytes(keyLen);
            long offset = br.ReadInt64();
            
            string key = Encoding.UTF8.GetString(keyBytes);
            _index.Add(new IndexEntry(key, offset));
        }
        
        return indexOffset;
    }
    
    /// <summary>
    /// Sucht einen Key in der SSTable mit Sparse-Index (binäre Suche + linearer Scan)
    /// </summary>
    public async Task<string?> GetAsync(string searchKey)
    {
        // 1. Binäre Suche im Sparse-Index um den Bereich zu finden
        int indexPos = _index.BinarySearch(new IndexEntry(searchKey, 0), 
            Comparer<IndexEntry>.Create((a, b) => string.CompareOrdinal(a.Key, b.Key)));
        
        // Bestimme Start- und End-Offset für den Scan-Bereich
        long startOffset;
        long endOffset;
        
        if (indexPos < 0)
        {
            // Key nicht exakt im Index gefunden, aber wir wissen wo er sein sollte
            int insertionPoint = ~indexPos;
            
            if (insertionPoint == 0)
            {
                // Vor dem ersten Index-Eintrag
                startOffset = SSTableFormat.HeaderSize;
            }
            else
            {
                // Zwischen zwei Index-Einträgen
                startOffset = _index[insertionPoint - 1].Offset;
            }
            
            if (insertionPoint >= _index.Count)
            {
                // Nach dem letzten Index-Eintrag - scanne bis zum Index
                endOffset = long.MaxValue; // Wird beim Lesen durch Index-Offset begrenzt
            }
            else
            {
                endOffset = _index[insertionPoint].Offset;
            }
        }
        else
        {
            // Exakter Match im Index (selten, aber möglich)
            startOffset = _index[indexPos].Offset;
            endOffset = indexPos + 1 < _index.Count ? _index[indexPos + 1].Offset : long.MaxValue;
        }
        
        // 2. Linearer Scan im Bereich
        await using var fs = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096);
        using var br = new BinaryReader(fs, Encoding.UTF8);
        
        // Begrenze End-Offset auf Index-Start
        if (endOffset > _indexOffset)
            endOffset = _indexOffset;
        
        // Scanne im Bereich
        fs.Seek(startOffset, SeekOrigin.Begin);
        
        while (fs.Position < endOffset)
        {
            int keyLen = br.ReadInt32();
            byte[] keyBytes = br.ReadBytes(keyLen);
            int valueLen = br.ReadInt32();
            
            string key = Encoding.UTF8.GetString(keyBytes);
            
            if (key == searchKey)
            {
                // Key gefunden!
                byte[] valueBytes = br.ReadBytes(valueLen);
                return Encoding.UTF8.GetString(valueBytes);
            }
            
            // Key nicht gefunden, skip value und weiter
            fs.Seek(valueLen, SeekOrigin.Current);
        }
        
        return null; // Key nicht gefunden
    }
    
    /// <summary>
    /// Gibt alle Keys in dieser SSTable zurück (für Debugging/Stats)
    /// </summary>
    public IEnumerable<string> GetAllKeys()
    {
        return _index.Select(e => e.Key);
    }
    
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
    }
}

