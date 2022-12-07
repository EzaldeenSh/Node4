package server;

import communication.Message;
import communication.MessageGenerator;
import data.Node;
import data.NodesDaoUser;
import data.User;
import json.JSONFunctions;
import json.OwnershipHandler;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

public class ClientHandler implements Runnable{
    private final ObjectInputStream fromClient;
    private final ObjectOutputStream toClient;
    private JSONFunctions jsonFunctions;
    private final String identity;
    private final Socket client;
    private final Node thisNode;
    private NodesDaoUser nodesDaoUser;
    private int selection;

    public ClientHandler(ObjectInputStream fromClient, ObjectOutputStream toClient,String identity, Socket client) {
        this.fromClient = fromClient;
        this.toClient = toClient;
        this.identity = identity;
        this.client = client;
        thisNode = Node.getInstance();
        initiate();
    }
    private void initiate(){
        nodesDaoUser = new NodesDaoUser();
        jsonFunctions = new JSONFunctions();
    }
    private void addClient(){
        thisNode.setNumberOfConnectedUsers(thisNode.getNumberOfConnectedUsers() + 1);
        nodesDaoUser.updateNodeStatus(thisNode);

    }
    private void removeClient(){
        thisNode.setNumberOfConnectedUsers(thisNode.getNumberOfConnectedUsers()-1);
        nodesDaoUser.updateNodeStatus(thisNode);
    }
    private boolean validateUser(){
        try {
            String username = (String) fromClient.readObject();
            String password = (String) fromClient.readObject();
            return  new ClientValidator().validateClient(new User(username , password));
        } catch (IOException | ParseException | ClassNotFoundException e) {
                return false;
        }
    }
    private void createDatabase(){

        try {
            String databaseName = (String) fromClient.readObject();
            boolean result = jsonFunctions.createDatabase(databaseName);
            if(result){
                Message message = new MessageGenerator().generateCreateDatabaseMessage(databaseName);
                thisNode.notifyObservers(message);
            }
            toClient.writeObject(result);
            toClient.flush();
            selection = fromClient.readInt();

        } catch (IOException | ClassNotFoundException ignored) {

        }
    }
    private void createCollection() {
        try {
            String databaseName = (String) fromClient.readObject();
            String collectionName = (String) fromClient.readObject();
            JSONObject jsonSchema = (JSONObject) fromClient.readObject();

            boolean result = jsonFunctions.createCollection(databaseName, collectionName, jsonSchema);
            if (result) {
                Message message = new MessageGenerator().generateCreateCollectionMessage(databaseName, collectionName, jsonSchema);
                thisNode.notifyObservers(message);
            }
            toClient.writeObject(result);
            toClient.flush();
            selection = fromClient.readInt();
        } catch (IOException | ClassNotFoundException ignored) {

        }
    }
    private void readObjectByIndex(){
        try {
            String databaseName = (String) fromClient.readObject();
            String collectionName = (String) fromClient.readObject();
            int index = (int) fromClient.readObject();
            JSONObject jsonObject = jsonFunctions.readObjectByIndex(databaseName ,collectionName , index);
            toClient.writeObject(jsonObject);
            toClient.flush();
            selection = fromClient.readInt();
        } catch (IOException | ClassNotFoundException ignored) {

        }
    }
    private void readCollection(){
        try {
            String databaseName = (String) fromClient.readObject();
            String collectionName = (String) fromClient.readObject();

             JSONArray jsonArray = jsonFunctions.readCollection(databaseName, collectionName);
            toClient.writeObject(jsonArray);
            toClient.flush();
            selection = fromClient.readInt();
        } catch (IOException | ClassNotFoundException ignored) {
        }
    }
   private void writeObject() {
       try {
           String databaseName = (String) fromClient.readObject();
           String collectionName = (String) fromClient.readObject();
           JSONObject object = (JSONObject) fromClient.readObject();
           if(isMine(databaseName ,collectionName)){
                performWrite(databaseName , collectionName , object);
           }
           else {
               requestWrite(databaseName ,collectionName, object);
           }
       } catch (IOException | ClassNotFoundException e) {
           throw new RuntimeException(e);
       }
   }
    private void performWrite(String databaseName, String collectionName, JSONObject object) {
        try {

            boolean result = jsonFunctions.writeDocument(databaseName, collectionName, object);
            if (result) {
                Message message = new MessageGenerator().generateWriteMessage(databaseName, collectionName, object);
                thisNode.notifyObservers(message);
            }
            toClient.writeObject(result);
            toClient.flush();
            if(identity.equals("ClientNode"))
                selection = 0;
            else
                selection = fromClient.readInt();
        } catch (IOException ignored) {
        }
    }
    private void requestWrite(String databaseName, String collectionName, JSONObject object){
        boolean result;
        int portNumber = new OwnershipHandler().getOwnerPortNumber(databaseName ,collectionName);
        if(portNumber == -1){
            try {
                toClient.writeObject(false);
                selection = fromClient.readInt();
                return;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            Socket socket = new Socket("localhost" , portNumber);
            ObjectOutputStream toNode = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream fromNode = new ObjectInputStream(socket.getInputStream());
            toNode.writeObject("ClientNode");
            toNode.writeObject("admin");
            toNode.writeObject("admin");
            toNode.flush();
            Boolean isValid = (Boolean) fromNode.readObject();
            if (isValid) {
                toNode.writeInt(5);
                toNode.writeObject(databaseName);
                toNode.writeObject(collectionName);
                toNode.writeObject(object);
                toNode.flush();
                result = (Boolean) fromNode.readObject();
                toClient.writeObject(result);
                toClient.flush();
                Thread.sleep(500);
                socket.close();
                selection = fromClient.readInt();
            }
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private boolean isMine(String databaseName, String collection){
        return new OwnershipHandler().getCollectionOwner(databaseName ,collection).equals(thisNode.getNodeID());
    }

    private void updateObject() {
        try {
            String databaseName = (String) fromClient.readObject();
            String collectionName = (String) fromClient.readObject();
            int index = fromClient.readInt();
            JSONObject jsonObject = (JSONObject) fromClient.readObject();

            boolean result = jsonFunctions.updateObject(databaseName, collectionName, index, jsonObject);
            if (result) {
                Message message = new MessageGenerator().generateUpdateMessage(databaseName, collectionName, index, jsonObject);
                thisNode.notifyObservers(message);
            }
            toClient.writeObject(result);
            toClient.flush();
            selection = fromClient.readInt();
        } catch (IOException | ClassNotFoundException ignored) {

        }
    }
    private void deleteCollection(){
        try {
            String databaseName = (String) fromClient.readObject();
            String collectionName = (String) fromClient.readObject();
            boolean result = jsonFunctions.deleteCollection(databaseName, collectionName);
            if (result) {
                Message message = new MessageGenerator().generateDeleteCollectionMessage(databaseName, collectionName);
                thisNode.notifyObservers(message);
            }
            toClient.writeObject(result);
            toClient.flush();
            selection = fromClient.readInt();
        } catch (IOException | ClassNotFoundException ignored) {
        }
    }
    private void deleteDatabase() {
        try{
            String databaseName = (String) fromClient.readObject();
            boolean result = jsonFunctions.deleteDatabase(databaseName);

            if (result) {
                Message message = new MessageGenerator().generateDeleteDatabaseMessage(databaseName);
                thisNode.notifyObservers(message);
            }

            toClient.writeObject(result);
            toClient.flush();
            selection = fromClient.readInt();
        } catch (IOException | ClassNotFoundException ignored) {
        }
    }
    private void createIndexOnASingleJSONProperty() {
        try {
            String databaseName = (String) fromClient.readObject();
            String collectionName = (String) fromClient.readObject();
            String propertyName = (String) fromClient.readObject();
            boolean result = jsonFunctions.createIndexOnAJSONProperty(databaseName, collectionName, propertyName);
            if (result) {
                Message message = new MessageGenerator().generateCreateIndexOnAJSONPropertyMessage(databaseName, collectionName, propertyName);
                thisNode.notifyObservers(message);
            }
            toClient.writeObject(result);
            toClient.flush();
            selection = fromClient.readInt();
        } catch (IOException | ParseException | ClassNotFoundException ignored) {
        }
    }
    private void getJSONPropertyIndexing(){
        try {

            String databaseName = (String) fromClient.readObject();
            String collectionName = (String) fromClient.readObject();
            String propertyName = (String) fromClient.readObject();
            String propertyValue = (String) fromClient.readObject();
            List<Long> indexes =  jsonFunctions.getJSONPropertyIndexing(databaseName, collectionName, propertyName, propertyValue);
            toClient.writeObject(indexes);
            toClient.flush();
            selection = fromClient.readInt();
        } catch (IOException | ClassNotFoundException ignored) {
        }
    }
    @Override
    public void run() {
        try{
            if(identity.equals("Client")){
                addClient();
            }
            boolean validation = validateUser();
            toClient.writeObject(validation);
            if(!validation) {
                System.out.println("Not validated");
                return;
            }
            selection = fromClient.readInt();
            while(selection != 0){
                switch (selection){
                    case 1:
                    {
                        createDatabase();
                        break;
                    }
                    case 2:
                    {
                        createCollection();
                        break;
                    }
                    case 3:
                    {
                        readObjectByIndex();
                        break;
                    }
                    case 4 :{
                        readCollection();
                        break;
                    }
                    case 5 : {
                        writeObject();
                        break;
                    }
                    case 6:{
                        updateObject();
                        break;
                    }
                    case 7 :{
                        deleteCollection();
                        break;
                    }
                    case 8 :{
                       deleteDatabase();
                        break;
                    }
                    case 9:{
                        createIndexOnASingleJSONProperty();
                        break;
                    }
                    case 10:{
                        getJSONPropertyIndexing();
                        break;
                    }
                }
                if(selection == 0 ){
                    client.close();
                    break;
                }
            }
            if(identity.equals("Client")){
                removeClient();
            }
        } catch (IOException e ){
            e.printStackTrace();
        }


    }
}
