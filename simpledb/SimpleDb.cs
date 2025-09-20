using System.Text;
using MicroDb;

namespace microdb.simpledb;

public class SimpleDb : IDisposable
{
    private readonly string dataFile;
    private readonly string indexFile;
    private readonly IDbIndex dbIndex;
    private bool disposed;
    
    // Buffering for performance
    private readonly List<(string key, string value)> writeBuffer = new();
    private readonly SemaphoreSlim bufferSemaphore = new(1, 1);
    private const int BufferSize = 100000; // Flush every 1000 entries
    private const int MaxBufferSize = 500000; // Force flush at this size
    private Timer? flushTimer;
    private DateTime lastWriteTime = DateTime.UtcNow;
    private readonly TimeSpan flushDelay = TimeSpan.FromMilliseconds(50); // Flush after 50ms of inactivity

    public SimpleDb(IDbIndex dbIndex, string dataFile = "database.bin", string indexFile = "database.idx")
    {
        this.dbIndex = dbIndex;
        this.dataFile = dataFile;
        this.indexFile = indexFile;
    }

    public async Task SetAsync(string key, string value, bool immediateFlush = true)
    {
        await bufferSemaphore.WaitAsync();
        try
        {
            writeBuffer.Add((key, value));
            lastWriteTime = DateTime.UtcNow;
            
            // Force flush if buffer is too large
            if (writeBuffer.Count >= MaxBufferSize)
            {
                await FlushBufferAsync();
            }
            // Auto-flush if buffer reaches normal size
            else if (writeBuffer.Count >= BufferSize)
            {
                await FlushBufferAsync();
            }
            // For individual sets, flush immediately to ensure consistency
            else if (immediateFlush && writeBuffer.Count == 1)
            {
                await FlushBufferAsync();
            }
            // Set up delayed flush for small batches (2+ entries) or bulk operations
            else
            {
                ScheduleDelayedFlush();
            }
        }
        finally
        {
            bufferSemaphore.Release();
        }
    }
    
    public void Set(string key, string value, bool immediateFlush = true)
    {
        SetAsync(key, value, immediateFlush).GetAwaiter().GetResult();
    }
    
    private void ScheduleDelayedFlush()
    {
        // Cancel existing timer
        flushTimer?.Dispose();
        
        // Schedule new flush
        flushTimer = new Timer(async _ => 
        {
            await bufferSemaphore.WaitAsync();
            try
            {
                // Only flush if we haven't written anything recently
                if (DateTime.UtcNow - lastWriteTime >= flushDelay && writeBuffer.Count > 0)
                {
                    await FlushBufferAsync();
                }
            }
            finally
            {
                bufferSemaphore.Release();
            }
        }, null, (int)flushDelay.TotalMilliseconds, Timeout.Infinite);
    }
    
    public async Task FlushAsync()
    {
        await bufferSemaphore.WaitAsync();
        try
        {
            await FlushBufferAsync();
        }
        finally
        {
            bufferSemaphore.Release();
        }
    }
    
    public void Flush()
    {
        FlushAsync().GetAwaiter().GetResult();
    }
    
    private async Task FlushBufferAsync()
    {
        if (writeBuffer.Count == 0) return;
        
        // Stop the timer
        flushTimer?.Dispose();
        flushTimer = null;
        
        // Create a copy of the buffer to avoid holding the semaphore during I/O
        var bufferCopy = writeBuffer.ToList();
        writeBuffer.Clear();
        
        // Write all buffered entries in one batch using async I/O
        using var dataFs = new FileStream(dataFile, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 4096, useAsync: true);
        using var indexFs = new FileStream(indexFile, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 4096, useAsync: true);
        using var dataWriter = new BinaryWriter(dataFs, Encoding.UTF8, leaveOpen: true);
        using var indexWriter = new BinaryWriter(indexFs, Encoding.UTF8, leaveOpen: true);
        
        foreach (var (key, value) in bufferCopy)
        {
            byte[] keyData = Encoding.UTF8.GetBytes(key);
            byte[] valueData = Encoding.UTF8.GetBytes(value);
            long offset = dataFs.Position;
            
            // Write to data file
            dataWriter.Write(keyData.Length);
            dataWriter.Write(keyData);
            dataWriter.Write(valueData.Length);
            dataWriter.Write(valueData);
            
            // Write to index file
            indexWriter.Write(keyData.Length);
            indexWriter.Write(keyData);
            indexWriter.Write(offset);
            
            // Update cache
            dbIndex.AddAndUpdateMetadata(key, offset, indexFile);
        }
        
        // Flush both streams asynchronously
        await dataFs.FlushAsync();
        await indexFs.FlushAsync();
    }
    
    public string? GetIndexed(string searchKey)
    {
        var index = dbIndex.Get(indexFile);

        if (!index.TryGetValue(searchKey, out var pos))
            return null;

        using var fs = new FileStream(dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var br = new BinaryReader(fs);
        fs.Seek(pos, SeekOrigin.Begin);

        int keyLen = br.ReadInt32();
        var keyBuf = br.ReadBytes(keyLen);
        int valueLen = br.ReadInt32();
        var valueBuf = br.ReadBytes(valueLen);
        return Encoding.UTF8.GetString(valueBuf);
    }

    public string? GetScan(string searchKey)
    {
        string? last = null;
        using var fs = new FileStream(dataFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
        using var br = new BinaryReader(fs);

        while (fs.Position < fs.Length)
        {
            int keyLen = br.ReadInt32();
            var keyBuf = br.ReadBytes(keyLen);
            int valueLen = br.ReadInt32();
            var valueBuf = br.ReadBytes(valueLen);
            
            string key = Encoding.UTF8.GetString(keyBuf);
            if (key == searchKey)
                last = Encoding.UTF8.GetString(valueBuf);
        }
        return last;
    }


    public void Clear()
    {
        if (File.Exists(dataFile)) File.Delete(dataFile);
        if (File.Exists(indexFile)) File.Delete(indexFile);
        dbIndex.Clear();
    }

    public void Dispose()
    {
        if (!disposed)
        {
            // Flush any remaining buffer before disposing
            Flush();
            flushTimer?.Dispose();
            bufferSemaphore?.Dispose();
            disposed = true;
        }
    }
}