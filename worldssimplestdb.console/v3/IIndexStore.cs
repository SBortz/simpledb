namespace worldssimplestdb.v3;

public interface IIndexStore
{
    void Load(Action<string>? feedback = null);
    bool TryGetValue(string key, out long offset);
    void WriteEntry(string key, long offset);
}