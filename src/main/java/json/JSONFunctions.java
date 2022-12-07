package json;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
public class JSONFunctions {
    private final PathsHandler pathsHandler;
    private final FilesHandler filesHandler;
    public JSONFunctions(){
        pathsHandler = new PathsHandler();
        filesHandler = new FilesHandler();
    }
    public synchronized boolean createDatabase(String databaseName){
        return filesHandler.createDatabaseFile(databaseName);

    }
    public synchronized boolean createCollection(String databaseName,String collectionName, JSONObject schema)  {
        if(exists(databaseName ,collectionName))
            return false;
        try {
            if(!filesHandler.createCollectionFile(databaseName ,collectionName))
                return false;
            else{
                boolean idResult = filesHandler.createIdFile(databaseName ,collectionName);
                boolean indexResult = filesHandler.createIndexFile(databaseName ,collectionName);
                boolean dataResult = filesHandler.createDataFile(databaseName ,collectionName);
                boolean schemaResult = filesHandler.createSchemaFile(databaseName ,collectionName, schema);
                boolean affinityResult = filesHandler.createAffinityFile(databaseName ,collectionName);

                new AffinityAssigner().assignAffinity(new File(pathsHandler.getAffinityPath(databaseName ,collectionName)));
                return idResult && indexResult && dataResult && schemaResult && affinityResult;
            }
        } catch (IOException e) {
           return false;
        }
    }
    public JSONArray readCollection(String databaseName, String collectionName) {
        if (!exists(databaseName, collectionName)) {
            return new JSONArray();
        }
        JSONArray jsonIDs;
        JSONArray result;
        try {
            jsonIDs = readIds(databaseName, collectionName);

            result = new JSONArray();
            JSONParser jsonParser = new JSONParser();
            for (Object object : jsonIDs) {
                JSONObject currentID = (JSONObject) object;
                if (currentID.get("_id_").equals("")) {
                    continue;
                }
                int startByte = Integer.parseInt(String.valueOf(currentID.get("startByte")));
                int endByte = Integer.parseInt(String.valueOf(currentID.get("endByte")));
                String text = filesHandler.getData(databaseName, collectionName, startByte, endByte);
                JSONObject jsonObject = (JSONObject) jsonParser.parse(text);
                result.add(jsonObject);
            }

        } catch (IOException | ParseException e) {
            return new JSONArray();
        }

        return result;
    }
    public JSONObject readObjectByIndex(String databaseName, String collectionName, int index){
        if(!exists(databaseName ,collectionName))
            return new JSONObject();

        JSONObject idObject = null;
        JSONObject json;
        String requestedId = databaseName + "_" + collectionName + "_" + index;
        try {
            JSONArray jsonArray = readIds(databaseName,collectionName);
            for(Object o : jsonArray){
                JSONObject current = (JSONObject) o;
                if(current.get("_id_").toString().equalsIgnoreCase(requestedId)){
                    idObject = current;
                    break;
                }
            }
            if(idObject == null)
                return new JSONObject();
            int startIndex =  Integer.parseInt(String.valueOf(idObject.get("startByte")));
            int endIndex = Integer.parseInt(String.valueOf(idObject.get("endByte")));

            String text = filesHandler.getData(databaseName ,collectionName, startIndex , endIndex);
            JSONParser parser = new JSONParser();
            json = (JSONObject) parser.parse(text);
        } catch (IOException | ParseException e) {
            return new JSONObject();
        }
        return json;
    }
    public boolean writeDocument(String databaseName, String collectionName, JSONObject document)  {
        if(!exists(databaseName ,collectionName))
            return false;

        String schemaString;
        try {
            schemaString = filesHandler.getSchema(databaseName ,collectionName);


        JSONParser jsonParser = new JSONParser();
        JSONObject schema = (JSONObject) jsonParser.parse(schemaString);

        if(! new ObjectValidator().validateObject(document , schema)){
            System.out.println("invalid schema");
            return false;
        }
        int currentIndex;
        try {
            currentIndex = filesHandler.getCurrentIndex(databaseName ,collectionName);
            filesHandler.increaseIndex(databaseName ,collectionName);
        } catch (IOException e) {
            return false;
        }
        return writeOnIndex(databaseName , collectionName , currentIndex , document);
        } catch (FileNotFoundException | ParseException e) {
            return false;
        }
    }
    public boolean updateObject(String databaseName, String collectionName, int index ,JSONObject jsonObject) {
        if (!exists(databaseName, collectionName))
            return false;
        JSONArray idsArray = readIds(databaseName, collectionName);
        JSONObject idObject = null;
        for (Object o : idsArray) {
            JSONObject current = (JSONObject) o;
            if (current.get("_id_").toString().equalsIgnoreCase(databaseName + "_" + collectionName + "_" + index)) {
                idObject = current;
                break;
            }
        }
        if (idObject == null)
            return false;
        idsArray.remove(idObject);
        if(!writeIdFile(databaseName, collectionName, idsArray))
             return false;

        return writeOnIndex(databaseName, collectionName, index, jsonObject);

    }
    public boolean deleteCollection(String databaseName, String collectionName){
        if(!exists(databaseName ,collectionName))
            return false;
        return filesHandler.deleteCollection(databaseName ,collectionName);

    }
    public boolean deleteDatabase(String databaseName){
        return filesHandler.deleteDatabase(databaseName);
    }
    private JSONArray readIds(String databaseName, String collectionName)  {
        if(!exists(databaseName , collectionName)){
            return new JSONArray();
        }
        try {
        String ids = filesHandler.getAllIds(databaseName ,collectionName);
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
        } catch (FileNotFoundException | ParseException e) {
            return new JSONArray();
        }
    }
    private boolean writeOnIndex(String databaseName, String collectionName, int index, JSONObject object){
        if(!exists( databaseName ,collectionName))
            return false;
        long startByte;
        long endByte;
        try {
           long[] bytes = filesHandler.writeDataOnIndex(databaseName ,collectionName ,object);
           startByte = bytes[0];
           endByte = bytes[1];
            JSONObject idObject = new JSONObject();
            idObject.put("_id_", databaseName + "_"+collectionName + "_" + index);
            idObject.put("startByte", startByte);
            idObject.put("endByte", endByte);
            JSONArray ids = readIds(databaseName , collectionName);
            ids.add(idObject);
            return writeIdFile(databaseName , collectionName , ids);
        } catch (IOException e) {
            return false;
        }

    }

    private boolean writeIdFile(String databaseName , String collectionName , JSONArray jsonArray){
        if(!exists(databaseName ,collectionName))
            return false;
        try {
           if(!filesHandler.writeIdFile(databaseName ,collectionName,jsonArray))
               return false;
        } catch (IOException e) {
            return false;
        }
        return true;
    }
    public boolean createIndexOnAJSONProperty(String databaseName, String collectionName, String property) throws IOException, ParseException {
        if(!exists(databaseName , collectionName))
            return false;
        if(filesHandler.indexingExists(databaseName ,collectionName ,property))
            return false;
        if(!filesHandler.propertyExists(databaseName ,collectionName , property))
            return false;

        HashMap<String, ArrayList<Integer>> indexingMap = new HashMap<>();
        JSONArray ids = readIds(databaseName , collectionName);
        for(Object obj : ids){

            JSONObject idObject = (JSONObject) obj;

            String id = (String) idObject.get("_id_");
            String indexString = id.substring(id.lastIndexOf("_") +1);
            int objectIndex = Integer.parseInt(indexString);
            JSONObject dataObject = readObjectByIndex(databaseName ,collectionName ,objectIndex);
            String key = String.valueOf( dataObject.get(property));
            ArrayList<Integer> arrayList;
            if(indexingMap.containsKey(key)){
                arrayList = indexingMap.get(key);
            }
            else {
                arrayList = new ArrayList<>();
            }
            arrayList.add(objectIndex);
            indexingMap.put(key , arrayList);
        }
        JSONObject resultObject = new JSONObject();
        if(!indexingMap.isEmpty()) {
            for (String mapKey : indexingMap.keySet()) {
                resultObject.put(mapKey, indexingMap.get(mapKey));
            }

            return filesHandler.createIndexOnAJSONPropertyFile(databaseName , collectionName , property , resultObject);
        }
        return false;
    }
    public ArrayList<Long> getJSONPropertyIndexing(String databaseName, String collectionName, String propertyName, String value) {
        if (!exists(databaseName, collectionName))
            return new ArrayList<>();

        if (!filesHandler.indexingExists(databaseName, collectionName, propertyName))
            return new ArrayList<>();

        String jsonString = filesHandler.getIndexing(databaseName, collectionName, propertyName);
        if (jsonString.equals(""))
            return new ArrayList<>();
        try {
            JSONObject jsonObject = (JSONObject) new JSONParser().parse(jsonString);
            Object val = jsonObject.get(value);
            if (val != null)
                return (ArrayList<Long>) jsonObject.get(value);
            else
                return new ArrayList<>();

        }catch ( ParseException e){
            return new ArrayList<>();
        }
    }
    private boolean exists(String databaseName , String collectionName){
        String path = pathsHandler.getCollectionPath(databaseName ,collectionName);
        File file = new File(path);
        return file.exists();
    }
}
