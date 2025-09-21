namespace worldssimplestdb.v3;

public interface IIndexStore
{
    Dictionary<string, long> Get();
    Dictionary<string, long> GetWithFeedback(Action<string>? feedback = null);
    bool TryGetValue(string key, out long offset);
    void WriteEntry(string key, long offset);
}