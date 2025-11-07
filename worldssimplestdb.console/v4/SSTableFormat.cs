namespace worldssimplestdb.v4;

/// <summary>
/// Konstanten und Format-Definitionen für SSTable-Dateien
/// </summary>
public static class SSTableFormat
{
    /// <summary>
    /// Magic Number am Anfang jeder SSTable-Datei zur Validierung
    /// "SSTB" = 0x53535442
    /// </summary>
    public const int MagicNumber = 0x53535442;
    
    /// <summary>
    /// Aktuelle Format-Version
    /// </summary>
    public const int Version = 1;
    
    /// <summary>
    /// Standard-Größe der Memtable bevor Flush (in Bytes)
    /// </summary>
    public const long DefaultMemtableFlushSizeBytes = 100 * 1024 * 1024; // ~100 MB

    /// <summary>
    /// Sparse-Index Dichte: Nur jeder N-te Key wird im Index gespeichert
    /// z.B. SparseIndexDensity = 16 bedeutet: Nur jeder 16. Key ist im Index
    /// </summary>
    public const int SparseIndexDensity = 16;

    public const int HeaderSize = 24; // Erweitert um IndexEntryCount (4 Bytes)
}

