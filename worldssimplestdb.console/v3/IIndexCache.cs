namespace worldssimplestdb.v3;

public interface IIndexCache
{
    Dictionary<string, long> Get(string indexFile);
    Dictionary<string, long> GetWithFeedback(string indexFile, Action<string>? feedback = null);
    bool TryGetValue(string key, string indexFile, out long offset);
    void Add(string key, long offset);
    void AddAndUpdateMetadata(string key, long offset, string indexFile);
    void UpdateMetadata(long length, DateTime lastWriteTime);
    void WriteEntry(string key, long offset);
    void Clear();
}