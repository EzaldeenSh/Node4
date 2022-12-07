package communication;

import json.JSONFunctions;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class MessageExecutor {
    private final JSONFunctions jsonFunctions;
    public MessageExecutor(){
        jsonFunctions = new JSONFunctions();
    }
    public void executeMessage(Message message) throws IOException, ParseException {

        String function = MessageTranslator.getFunction(message);
        String[] params = MessageTranslator.getParams(message);

        switch (function){
            case "CreateDatabase":  {
                String databaseName = params[0];
                jsonFunctions.createDatabase(databaseName);
                break;
            }
            case "CreateCollection":{
                try {
                    JSONParser jsonParser = new JSONParser();

                    String databaseName = params[0];
                    String collectionName = params[1];
                    String schema = params[2];

                    JSONObject jsonSchema = (JSONObject) jsonParser.parse(schema);
                    jsonFunctions.createCollection(databaseName , collectionName , jsonSchema);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
            case "WriteObject":{
                JSONParser jsonParser = new JSONParser();
                String databaseName = params[0];
                String collectionName = params[1];
                try {
                    JSONObject jsonObject = (JSONObject) jsonParser.parse(params[2]);
                    jsonFunctions.writeDocument(databaseName , collectionName , jsonObject);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
            case "UpdateObject":{
                JSONParser jsonParser = new JSONParser();
                String databaseName = params[0];
                String collectionName = params[1];
                int index = Integer.parseInt(params[2]);
                try {
                    JSONObject jsonObject = (JSONObject) jsonParser.parse(params[3]);
                    jsonFunctions.updateObject(databaseName , collectionName ,index , jsonObject);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
            case "DeleteCollection":{
                String databaseName = params[0];
                String collectionName = params[1];
                jsonFunctions.deleteCollection(databaseName , collectionName);
                break;}
            case "DeleteDatabase":  {
                String databaseName = params[0];
                jsonFunctions.deleteDatabase(databaseName);
                break;
            }
            case "CreateIndex": {
                String databaseName = params[0];
                String collectionName = params[1];
                String property = params[2];
                jsonFunctions.createIndexOnAJSONProperty(databaseName ,collectionName ,property);
                break;
            }
        }

    }
}
