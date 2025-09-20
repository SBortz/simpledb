namespace MicroDb;

public interface IIndexCache
{
    Dictionary<string, long> Get(string indexFile);
    Dictionary<string, long> GetWithFeedback(string indexFile, Action<string>? feedback = null);
    void Add(string key, long offset);
    void AddAndUpdateMetadata(string key, long offset, string indexFile);
    void UpdateMetadata(long length, DateTime lastWriteTime);
    void Clear();
}