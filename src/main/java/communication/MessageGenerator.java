package communication;

import org.json.simple.JSONObject;

public class MessageGenerator {
    public Message generateCreateDatabaseMessage(String databaseName){
        Message message = new Message();
        String function = "CreateDatabase";
        String[] params = new String[1];
        params[0] = databaseName;
        message.setFunction(function);
        message.setParams(params);
        return message;

    }
    public Message generateCreateCollectionMessage(String databaseName, String collectionName, JSONObject schema){
        Message message = new Message();
        String function = "CreateCollection";
        String[] params = new String[3];
        params[0] = databaseName;
        params[1] = collectionName;
        params[2] = schema.toJSONString();
        message.setFunction(function);
        message.setParams(params);
        return message;
    }
    public Message generateWriteMessage(String databaseName, String collectionName, JSONObject jsonObject){
        Message message = new Message();
        String function = "WriteObject";
        String[] params = new String[3];
        params[0] = databaseName;
        params[1] = collectionName;
        params[2] = jsonObject.toJSONString();
        message.setFunction(function);
        message.setParams(params);
        return message;
    }
    public Message generateUpdateMessage(String databaseName, String collectionName, int index, JSONObject jsonObject){
        Message message = new Message();
        String function = "UpdateObject";
        String[] params = new String[4];
        params[0] = databaseName;
        params[1] = collectionName;
        params[2] = String.valueOf(index);
        params[3] = jsonObject.toJSONString();
        message.setFunction(function);
        message.setParams(params);
        return message;
    }
    public Message generateDeleteCollectionMessage(String databaseName, String collectionName){
        Message message = new Message();
        String function = "DeleteCollection";
        String params[] = new String[2];
        params[0] = databaseName;
        params[1] = collectionName;
        message.setFunction(function);
        message.setParams(params);
        return message;
    }
    public Message generateDeleteDatabaseMessage(String databaseName){
        Message message = new Message();
        String function = "DeleteDatabase";
        String[] params = new String[1];
        params[0] = databaseName;
        message.setFunction(function);
        message.setParams(params);
        return message;
    }
}
