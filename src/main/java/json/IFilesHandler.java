package json;

public interface IFilesHandler {
    boolean createDatabaseFile(String databaseName);
    boolean deleteDatabase(String databaseName);
    boolean createCollectionFile(String databaseName, String collectionName);
    boolean deleteCollection(String databaseName, String collectionName);
}
