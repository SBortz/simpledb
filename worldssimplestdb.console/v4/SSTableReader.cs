using System.Text;

namespace worldssimplestdb.v4;

/// <summary>
/// Liest aus einer immutable SSTable-Datei mit effizientem Index-basierten Lookup
/// </summary>
public class SSTableReader : IDisposable
{
    private readonly string _filename;
    private readonly List<IndexEntry> _index = new();
    private bool _disposed = false;
    
    public string Filename => _filename;
    public int EntryCount => _index.Count;
    public DateTime CreationTime { get; private set; }
    
    private record IndexEntry(string Key, long Offset);
    
    public SSTableReader(string filename)
    {
        _filename = filename;
        CreationTime = File.GetCreationTimeUtc(filename);
        LoadIndex();
    }
    
    /// <summary>
    /// Lädt den Index aus der SSTable-Datei in den Speicher
    /// </summary>
    private void LoadIndex()
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
        long indexOffset = br.ReadInt64();
        
        // 2. Springe zum Index und lade alle Index-Einträge
        fs.Seek(indexOffset, SeekOrigin.Begin);
        
        for (int i = 0; i < entryCount; i++)
        {
            int keyLen = br.ReadInt32();
            byte[] keyBytes = br.ReadBytes(keyLen);
            long offset = br.ReadInt64();
            
            string key = Encoding.UTF8.GetString(keyBytes);
            _index.Add(new IndexEntry(key, offset));
        }
    }
    
    /// <summary>
    /// Sucht einen Key in der SSTable mit binärer Suche
    /// </summary>
    public async Task<string?> GetAsync(string searchKey)
    {
        // Binäre Suche im Index (Keys sind sortiert!)
        int index = _index.BinarySearch(new IndexEntry(searchKey, 0), 
            Comparer<IndexEntry>.Create((a, b) => string.CompareOrdinal(a.Key, b.Key)));
        
        if (index < 0)
            return null; // Key nicht gefunden
        
        // Key gefunden, lese den Value von der gespeicherten Position
        long offset = _index[index].Offset;
        
        await using var fs = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096);
        using var br = new BinaryReader(fs, Encoding.UTF8);
        
        fs.Seek(offset, SeekOrigin.Begin);
        
        // Lese Entry (skip key, read value)
        int keyLen = br.ReadInt32();
        fs.Seek(keyLen, SeekOrigin.Current); // Skip key
        
        int valueLen = br.ReadInt32();
        byte[] valueBytes = br.ReadBytes(valueLen);
        
        return Encoding.UTF8.GetString(valueBytes);
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

