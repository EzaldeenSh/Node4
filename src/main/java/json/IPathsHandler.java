package json;

public interface IPathsHandler {
    String getCollectionPath(String databaseName, String collectionName);
    String getDatabasePath(String databaseName);
}
