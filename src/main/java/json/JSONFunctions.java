package json;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Scanner;

public class JSONFunctions {
    private final PathsHandler pathsHandler;
    private static JSONFunctions instance;
    private JSONFunctions(){
        pathsHandler = new PathsHandler();
    }
    public static JSONFunctions getInstance(){
        if (instance == null)
            instance = new JSONFunctions();
        return instance;
    }
    public boolean createDatabase(String databaseName){
        String path = pathsHandler.getDatabasePath(databaseName);
        File file = new File(path);
        return  file.mkdir();

    }
    public boolean createCollection(String databaseName,String collectionName, JSONObject schema) throws IOException {
        if(exists(databaseName ,collectionName))
            return false;
        String path = pathsHandler.getCollectionPath(databaseName , collectionName);
        File file = new File(path);
        if(file.exists())
            return false;
        else{
            if(!file.mkdir())
                return false;
            File idFile = new File(path + "\\idFile.json");
            if (!idFile.createNewFile())
                return false;
            FileWriter idWriter = new FileWriter(idFile);
            idWriter.write("[]");
            idWriter.close();
            File dataFile =new File(path+"\\data.json");
            if(!dataFile.createNewFile())
                return false;
            FileWriter dataFileWriter = new FileWriter(dataFile);
            dataFileWriter.write("[]");
            dataFileWriter.close();
            File indexFile = new File(path + "\\index.txt");
            FileWriter indexFileWriter = new FileWriter(indexFile);
            indexFileWriter.write("0");
            indexFileWriter.close();
            File schemaFile = new File(path+"\\schema.json");

            if(!schemaFile.createNewFile())
                return false;
            FileWriter schemaWriter = new FileWriter(schemaFile);
            schemaWriter.write(schema.toString());
            schemaWriter.close();
            File affinityFile = new File(path + "\\affinity.txt");
            if(!affinityFile.createNewFile())
                return false;
            new AffinityAssigner().assignAffinity(affinityFile);
            return true;
        }
    }

    public JSONArray readCollection(String databaseName, String collectionName) throws IOException, ParseException {
        if(!exists(databaseName ,collectionName)){
            return new JSONArray();
        }
        String path  = pathsHandler.getDataFilePath(databaseName ,collectionName);
        JSONArray jsonIDs = readIds(databaseName , collectionName);
        RandomAccessFile randomAccessFile = new RandomAccessFile(path , "r");
        JSONArray result = new JSONArray();
        JSONParser jsonParser = new JSONParser();
        for(Object object : jsonIDs){
            JSONObject currentID = (JSONObject) object;
            if(currentID.get("_id_").equals("")){
                continue;
            }
            int startByte = Integer.parseInt(String.valueOf(currentID.get("startByte")));
            int endByte = Integer.parseInt(String.valueOf(currentID.get("endByte")));
            int length = endByte - startByte;
            randomAccessFile.seek(startByte);
            byte[] bytes = new byte[length];
            randomAccessFile.read(bytes);
            String text = new String(bytes);
            JSONObject jsonObject = (JSONObject) jsonParser.parse(text);
            result.add(jsonObject);

        }
        randomAccessFile.close();

        return result;
    }
    public JSONObject readObjectByIndex(String databaseName, String collectionName, int index){
        if(!exists(databaseName ,collectionName))
            return new JSONObject();

        String path = pathsHandler.getDataFilePath(databaseName , collectionName);
        JSONObject idObject = null;
        JSONObject json;
        try {
            JSONArray jsonArray = readIds(databaseName,collectionName);
            for(Object o : jsonArray){
                JSONObject current = (JSONObject) o;
                if(current.get("_id_").equals(databaseName + "_" + collectionName + "_" + index)){
                    idObject = current;
                    break;
                }
            }
            if(idObject == null)
                return new JSONObject();

            int startIndex =  Integer.parseInt(String.valueOf(idObject.get("startByte")));
            int endIndex = Integer.parseInt(String.valueOf(idObject.get("endByte")));
            byte[] bytes;

            RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r");
            randomAccessFile.seek(startIndex);
            int length = endIndex - startIndex;
            bytes = new byte[length];
            randomAccessFile.read(bytes);
            randomAccessFile.close();

            String text = new String(bytes);
            JSONParser parser = new JSONParser();
            json = (JSONObject) parser.parse(text);
        } catch (IOException | ParseException e) {
            return new JSONObject();
        }
        return json;
    }
    public boolean writeDocument(String databaseName, String collectionName, JSONObject document) throws IOException, ParseException {
        if(!exists(databaseName ,collectionName))
            return false;
        JSONParser jsonParser = new JSONParser();
        String schemaPath = pathsHandler.getSchemaFilePath(databaseName , collectionName);
        File schemaFile = new File(schemaPath);
        Scanner schemaScanner = new Scanner(schemaFile);
        String schemaString = schemaScanner.nextLine();
        schemaScanner.close();
        JSONObject schema = (JSONObject) jsonParser.parse(schemaString);

        if(! new ObjectValidator().validateObject(document , schema)){
            System.out.println("invalid schema");
            return false;
        }

        String indexPath = pathsHandler.getIndexFilePath(databaseName , collectionName);
        int currentIndex;
        try {
            Scanner scanner = new Scanner(new File(indexPath));
            currentIndex = scanner.nextInt();
            scanner.close();
            FileWriter fileWriter = new FileWriter(indexPath);
            fileWriter.write(String.valueOf((currentIndex+1)));
            fileWriter.close();
        } catch (IOException e) {
            System.out.println("IOException happening");
            return false;
        }
        return writeOnIndex(databaseName , collectionName , currentIndex , document);

    }
    public boolean updateObject(String databaseName, String collectionName, int index ,JSONObject jsonObject) throws IOException, ParseException {
        if(!exists(databaseName ,collectionName))
            return false;

        JSONArray idsArray = readIds(databaseName , collectionName);
        JSONObject idObject = null;
        for(Object o : idsArray){
            JSONObject current = (JSONObject) o;
            if(current.get("_id_").equals(databaseName + "_" + collectionName + "_" + index)) {
                idObject = current;
                break;
            }
        }
        if(idObject== null)
            return false;
        idsArray.remove(idObject);
        writeIdFile(databaseName , collectionName , idsArray);

        writeOnIndex(databaseName , collectionName, index , jsonObject);





        return true;
    }

    public boolean deleteCollection(String databaseName, String collectionName){
        if(!exists(databaseName ,collectionName))
            return false;

        File directory = new File(pathsHandler.getCollectionPath(databaseName , collectionName));
        if(!directory.exists()){
            System.out.println("File does not exist");
            return false;
        }
        File[] files = directory.listFiles();
        if(files!=null)
            for(File file : files){
                file.delete();
            }
        return directory.delete();
    }
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
                file.delete();
            }

        return directory.delete();
    }
    private JSONArray readIds(String databaseName, String collectionName) throws IOException, ParseException {
        if(!exists(collectionName , databaseName)){
            return new JSONArray();
        }
        String path = pathsHandler.getIdFilePath(databaseName , collectionName);
        File idFile = new File(path);
        Scanner idScanner = new Scanner(idFile);
        String ids = idScanner.nextLine();
        idScanner.close();
        JSONParser jsonParser = new JSONParser();
        JSONArray jsonArray = (JSONArray) jsonParser.parse(ids);
        JSONArray result = new JSONArray();

        for( Object object : jsonArray){
            JSONObject jsonObject = (JSONObject) object;
            if(jsonObject.get("_id_").equals("")){
                continue;
            }
            result.add(jsonObject);
        }
        return result;
    }
    private boolean writeOnIndex(String databaseName, String collectionName, int index, JSONObject object){
        if(!exists( databaseName ,collectionName))
            return false;
        long startByte;
        long endByte;
        RandomAccessFile raf;

        String dataPath = pathsHandler.getDataFilePath(databaseName , collectionName);
        try {
            raf = new RandomAccessFile(new File(dataPath) , "rw");
            String data = object.toJSONString();
            startByte = raf.length()-1;
            raf.seek(startByte);
            if(raf.length()!=2) {
                raf.write(",".getBytes());
                startByte++;
            }
            raf.write(data.getBytes());
            endByte = raf.length();
            raf.write("]".getBytes());
            raf.close();
            JSONObject idObject = new JSONObject();
            idObject.put("_id_", databaseName + "_"+collectionName + "_" + index);
            idObject.put("startByte", startByte);
            idObject.put("endByte", endByte);
            JSONArray ids = readIds(databaseName , collectionName);
            ids.add(idObject);
            return writeIdFile(databaseName , collectionName , ids);




        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }

    }

    private boolean writeIdFile(String databaseName , String collectionName , JSONArray jsonArray){
        if(!exists(databaseName ,collectionName))
            return false;

        String path = pathsHandler.getIdFilePath(databaseName , collectionName);
        try {
            FileWriter fileWriter = new FileWriter(path);
            fileWriter.write(jsonArray.toJSONString());
            fileWriter.close();
        } catch (IOException e) {
            return false;
        }
        return true;
    }
    private boolean exists(String databaseName , String collectionName){
        String path = pathsHandler.getCollectionPath(databaseName ,collectionName);
        File file = new File(path);
        return file.exists();
    }
}
