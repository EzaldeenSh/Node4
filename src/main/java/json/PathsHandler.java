package json;

public class PathsHandler {
    private final String basePath = "C:\\Users\\User\\Desktop\\Outputs4";
    public String getDatabasePath(String databaseName){

        return basePath +"\\"+ databaseName;
    }
    public String getCollectionPath(String databaseName, String collectionName){
        return getDatabasePath(databaseName + "\\"+ collectionName);
    }
    public String getIndexFilePath(String databaseName, String collectionName){
        return getCollectionPath(databaseName , collectionName ) + "\\index.txt";
    }
    public String getIdFilePath(String databaseName, String collectionName){
        return getCollectionPath(databaseName , collectionName ) + "\\idFile.json";
    }
    public String  getDataFilePath(String databaseName, String collectionName){
        return getCollectionPath(databaseName , collectionName ) + "\\data.json";
    }
    public String getSchemaFilePath(String databaseName , String collectionName){
        return getCollectionPath(databaseName , collectionName ) + "\\schema.json";
    }
    public String getAffinityPath(String databaseName, String collectionName){
        return getCollectionPath(databaseName , collectionName) + "\\affinity.txt";
    }

    public String getBasePath() {
        return basePath;
    }
}
