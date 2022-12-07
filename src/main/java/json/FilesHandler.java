package json;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.locks.ReentrantLock;

public class FilesHandler implements IFilesHandler{
    private final PathsHandler pathsHandler;
    private final ReentrantLock reentrantLock;
    public FilesHandler(){
        pathsHandler = new PathsHandler();
        reentrantLock = new ReentrantLock();
    }
    @Override
    public boolean createDatabaseFile(String databaseName) {
        String path = pathsHandler.getDatabasePath(databaseName);
        reentrantLock.lock();
        boolean result =  new File(path).mkdir();
        reentrantLock.unlock();
        return result;
    }
    public boolean createAffinityFile(String databaseName, String collectionName) throws IOException {
        String path = pathsHandler.getAffinityPath(databaseName , collectionName);
        reentrantLock.lock();
        boolean result =  new File(path).createNewFile();
        reentrantLock.unlock();
        return result;
    }
    public boolean createCollectionFile(String databaseName, String collectionName) {
        String path = pathsHandler.getCollectionPath(databaseName , collectionName);
        reentrantLock.lock();
        boolean result = new File(path).mkdir();
        reentrantLock.unlock();
        return result;
    }
    public boolean createIdFile(String databaseName, String collectionName) throws IOException {
        String path = pathsHandler.getIdFilePath(databaseName , collectionName);
        File idFile = new File(path);
        reentrantLock.lock();
        boolean result = idFile.createNewFile();
        FileWriter idWriter = new FileWriter(idFile);
        idWriter.write("[]");
        idWriter.close();
        reentrantLock.unlock();
        return result;
    }
    public boolean createDataFile(String databaseName, String collectionName) throws IOException{
        String path = pathsHandler.getDataFilePath(databaseName , collectionName);
        File file = new File(path);
        reentrantLock.lock();
        boolean result = file.createNewFile();
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write("[]");
        fileWriter.close();
        reentrantLock.unlock();
        return result;
    }
    public boolean createIndexFile(String databaseName, String collectionName) throws IOException{
        String path = pathsHandler.getIndexFilePath(databaseName ,collectionName);
        File dataFile = new File(path);
        reentrantLock.lock();
        boolean result = dataFile.createNewFile();
        FileWriter fileWriter = new FileWriter(dataFile);
        fileWriter.write("0");
        fileWriter.close();
        reentrantLock.unlock();
        return result;
    }
    public boolean createSchemaFile(String databaseName , String collectionName, JSONObject schema) throws IOException {
        String path = pathsHandler.getSchemaFilePath(databaseName , collectionName);
         File schemaFile = new File(path);
         reentrantLock.lock();
         boolean result = schemaFile.createNewFile();
         FileWriter fileWriter = new FileWriter(schemaFile);
         fileWriter.write(schema.toJSONString());
         fileWriter.close();
         reentrantLock.unlock();
        return result;
    }
    public boolean createIndexOnAJSONPropertyFile(String databaseName, String collectionName, String property, JSONObject resultObject) throws IOException {
        String path = pathsHandler.getSingleJSONPropertyPath(databaseName , collectionName , property);
        File indexingFile = new File(path);
        reentrantLock.lock();
        boolean result = indexingFile.createNewFile();
        FileWriter fileWriter = new FileWriter(indexingFile);
        fileWriter.write(resultObject.toJSONString());
        fileWriter.close();
        reentrantLock.unlock();
        return result;
    }
    public boolean writeIdFile(String databaseName, String collectionName, JSONArray ids) throws IOException {
        String path = pathsHandler.getIdFilePath(databaseName ,collectionName);
        File idFile = new File(path);
        reentrantLock.lock();
        FileWriter fileWriter = new FileWriter(idFile);
        fileWriter.write(ids.toJSONString());
        fileWriter.close();
        reentrantLock.unlock();
        return true;
    }
    public String getData(String databaseName, String collectionName, int startByte, int endByte) throws IOException {
        String dataPath = pathsHandler.getDataFilePath(databaseName ,collectionName);
        RandomAccessFile raf = new RandomAccessFile(dataPath , "r");
        int length = endByte - startByte;
        raf.seek(startByte);
        byte[] bytes = new byte[length];
        raf.read(bytes);
        String result = new String(bytes);
        raf.close();
        return result;
    }
    public String getAllIds(String databaseName, String collectionName) throws FileNotFoundException {
        String path = pathsHandler.getIdFilePath(databaseName , collectionName);
        File idFile = new File(path);
        Scanner idScanner = new Scanner(idFile);
        String result = idScanner.nextLine();
        idScanner.close();
        return result;

    }
    public String getSchema(String databaseName, String collectionName) throws FileNotFoundException {
        String schemaPath = pathsHandler.getSchemaFilePath(databaseName , collectionName);
        File schemaFile = new File(schemaPath);
        Scanner scanner = new Scanner(schemaFile);
        String schemaString = scanner.nextLine();
        scanner.close();
        return schemaString;
    }
    public int getCurrentIndex(String databaseName, String collectionName) throws FileNotFoundException {
        String indexPath = pathsHandler.getIndexFilePath(databaseName ,collectionName);
        File indexFile = new File(indexPath);
        Scanner indexScanner = new Scanner(indexFile);
        int currentIndex = indexScanner.nextInt();
        indexScanner.close();
        return currentIndex;
    }
    public void increaseIndex(String databaseName, String collectionName) throws IOException {
        String indexPath = pathsHandler.getIndexFilePath(databaseName, collectionName);
        File indexFile = new File(indexPath);
        int currentIndex = getCurrentIndex(databaseName, collectionName);
        reentrantLock.lock();
            FileWriter fileWriter = new FileWriter(indexFile);
            fileWriter.write(String.valueOf(currentIndex + 1));
            fileWriter.close();
        reentrantLock.unlock();
    }
    public long[] writeDataOnIndex(String databaseName , String collectionName, JSONObject jsonObject) throws IOException {
        String path = pathsHandler.getDataFilePath(databaseName , collectionName);
        reentrantLock.lock();
        RandomAccessFile raf = new RandomAccessFile(new File(path), "rw");
        long startByte = (raf.length()-1);
        raf.seek(startByte);
        String data = jsonObject.toJSONString();
        if(raf.length() != 2){
            raf.write(",".getBytes());
            startByte++;
        }
        raf.write(data.getBytes());
        long endByte = raf.length();
        raf.write("]".getBytes());
        raf.close();
        reentrantLock.unlock();
        long[] bytes = new long[2];
        bytes[0] = startByte;
        bytes[1] = endByte;


        return bytes;

    }
    public boolean indexingExists(String databaseName, String collectionName, String propertyName){
        String path = pathsHandler.getSingleJSONPropertyPath(databaseName ,collectionName , propertyName);
        return new File(path).exists();
    }
    public String getIndexing(String databaseName, String collectionName, String propertyName){
        String path = pathsHandler.getSingleJSONPropertyPath(databaseName ,collectionName ,propertyName);
        File indexingFile = new File(path);
        try {
            Scanner scanner = new Scanner(indexingFile);
            String result = scanner.nextLine();
            scanner.close();
            return result;
        } catch (FileNotFoundException e) {
            return "";
        }
    }
    public boolean propertyExists(String databaseName, String collectionName, String property) throws FileNotFoundException, ParseException {
        String schemaPath = pathsHandler.getSchemaFilePath(databaseName ,collectionName);
        File schemaFile = new File(schemaPath);
        Scanner scanner = new Scanner(schemaFile);
        String schemaString = scanner.nextLine();
        scanner.close();
        JSONObject schema = (JSONObject) new JSONParser().parse(schemaString);
        for(Object key : schema.keySet()){
            if (key.toString().equals(property))
                return true;
        }
        return false;
    }
    @Override
    public boolean deleteDatabase(String databaseName){

        File directory = new File(pathsHandler.getDatabasePath(databaseName));
        if(!directory.exists()){
            return false;
        }
        File[] data = directory.listFiles();
        if(data != null)
            for(File file : data ){
                if(file.isDirectory()){
                    deleteCollection(databaseName,file.getName());
                }
               if(! file.delete())
                   return false;
            }

        return directory.delete();
    }
    @Override
    public boolean deleteCollection(String databaseName, String collectionName){
        File directory = new File(pathsHandler.getCollectionPath(databaseName , collectionName));
        if(!directory.exists()){
            return false;
        }
        File[] files = directory.listFiles();
        if(files!=null)
            for(File file : files){
                if(!file.delete())
                    return false;
            }
        return directory.delete();
    }

}