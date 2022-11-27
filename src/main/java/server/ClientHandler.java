package server;

import communication.Message;
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
public class ClientHandler implements Runnable{
    private final ObjectInputStream fromClient;
    private final ObjectOutputStream toClient;
    private final JSONFunctions jsonFunctions;
    private final String identity;
    private final Socket client;

    public ClientHandler(ObjectInputStream fromClient, ObjectOutputStream toClient,String identity, Socket client) {
        jsonFunctions = JSONFunctions.getInstance();
        this.fromClient = fromClient;
        this.toClient=toClient;
        this.identity = identity;
        this.client = client;

    }

    @Override
    public void run() {
        Node thisNode;
        NodesDaoUser nodesDaoUser;
        try {
            nodesDaoUser = NodesDaoUser.getInstance();
            thisNode = nodesDaoUser.getNode("node1");
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
        try{
            System.out.println("Client being handled");
            thisNode.setNumberOfConnectedUsers(thisNode.getNumberOfConnectedUsers() + 1);
            nodesDaoUser.updateNodeStatus(thisNode);
            String username = (String) fromClient.readObject();
            String password = (String) fromClient.readObject();

            boolean validation = new ClientValidator().validateClient(new User(username , password));
            toClient.writeObject(validation);
            if(!validation) {
                System.out.println("Not validated");
                return;
            }

            int selection;
            selection = fromClient.readInt();

            while(selection != 0){
                switch (selection){
                    case 1:
                    { //create database
                        System.out.println("database creation selected");
                        String databaseName = (String) fromClient.readObject();
                        boolean result = jsonFunctions.createDatabase(databaseName);

                        if(result){
                        Message message = new Message();
                        message.setFunction("CreateDatabase");
                        String[] params = new String[1];
                        params[0] = databaseName;
                        message.setParams(params);
                        thisNode.notifyObservers(message);
                        }
                        toClient.writeObject(result);
                        selection = fromClient.readInt();

                        break;
                    }
                    case 2:
                    { // create collection

                        String databaseName = (String) fromClient.readObject();
                        String collectionName = (String) fromClient.readObject();
                        JSONObject jsonSchema = (JSONObject) fromClient.readObject();


                        boolean result = jsonFunctions.createCollection(databaseName,collectionName , jsonSchema);
                        if(result){
                        Message message = new Message();
                        message.setFunction("CreateCollection");
                        String[] params = new String[3];
                        params[0] = databaseName;
                        params[1] = collectionName;
                        params[2] = jsonSchema.toJSONString();
                        message.setParams(params);
                        thisNode.notifyObservers(message);
                        }
                        toClient.writeObject(result);
                        selection = fromClient.readInt();
                        break;

                    }
                    case 3:
                    { //read object by index
                        String databaseName = (String) fromClient.readObject();
                        String collectionName = (String) fromClient.readObject();
                        int index = (int) fromClient.readObject();

                        JSONObject jsonObject =jsonFunctions.readObjectByIndex(databaseName, collectionName, index);

                        toClient.writeObject(jsonObject);
                        selection = fromClient.readInt();
                        break;

                    }
                    case 4 :{ //read collection
                        String databaseName = (String) fromClient.readObject();
                        String collectionName = (String) fromClient.readObject();
                        JSONArray jsonArray = jsonFunctions.readCollection(databaseName , collectionName);

                        toClient.writeObject(jsonArray);
                        selection = fromClient.readInt();
                        break;
                    }
                    case 5 : { //write object

                        String databaseName = (String) fromClient.readObject();
                        String collectionName = (String) fromClient.readObject();
                        JSONObject jsonObject = (JSONObject) fromClient.readObject();
                        boolean result;
                        String owner = new OwnershipHandler().getCollectionOwner(databaseName ,collectionName);
                        if(owner.equals("node1")){

                            result = jsonFunctions.writeDocument(databaseName , collectionName , jsonObject);
                            if (result){
                                Message message = new Message();
                                message.setFunction("WriteObject");
                                String[] params = new String[3];
                                params[0] = databaseName;
                                params[1] = collectionName;
                                params[2] = jsonObject.toJSONString();
                                message.setParams(params);
                                thisNode.notifyObservers(message);
                                System.out.println("Observers notified");
                            }
                            toClient.writeObject(result);

                            if(identity.equals("ClientNode")){
                                selection = 0;
                            } else selection = fromClient.readInt();

                        } else{

                            int ownerPortNumber = new OwnershipHandler().getOwnerPortNumber(databaseName , collectionName);
                            System.out.println("Attempting Connection On :" + ownerPortNumber);
                            Socket socket = new Socket("localhost" , ownerPortNumber);
                            System.out.println("Connected to " + ownerPortNumber);
                            ObjectOutputStream toNode =  new ObjectOutputStream(socket.getOutputStream());
                            ObjectInputStream fromNode = new ObjectInputStream(socket.getInputStream()) ;
                            toNode.writeObject("ClientNode");
                            toNode.writeObject("admin");
                            toNode.writeObject("admin");
                            Boolean isValid = (Boolean) fromNode.readObject();
                            if(isValid){
                                System.out.println("Node validated as a ClientNode");
                                toNode.writeInt(5);
                                toNode.writeObject(databaseName);
                                toNode.writeObject(collectionName);
                                toNode.writeObject(jsonObject);
                                result = (Boolean) fromNode.readObject();
                                toClient.writeObject(result);
                                Thread.sleep(500);
                                socket.close();
                                selection = fromClient.readInt();

                            }
                        }
                        break;
                    }
                    case 6:{//update object
                        String databaseName = (String) fromClient.readObject();
                        String collectionName = (String) fromClient.readObject();
                        int index = fromClient.readInt();
                        JSONObject jsonObject = (JSONObject) fromClient.readObject();

                        boolean result = jsonFunctions.updateObject(databaseName , collectionName , index , jsonObject);
                        if(result){
                        Message message = new Message();
                        message.setFunction("UpdateObject");
                        String[] params = new String[4];
                        params[0] = databaseName;
                        params[1] = collectionName;
                        params[2] = String.valueOf(index);
                        params[3] = jsonObject.toJSONString();
                        message.setParams(params);
                        thisNode.notifyObservers(message);
                        }
                        toClient.writeObject(result);
                        selection = fromClient.readInt();
                        break;
                    }
                    case 7 :{
                        //delete collection

                        String databaseName = (String) fromClient.readObject();
                        String collectionName = (String) fromClient.readObject();

                        boolean result = jsonFunctions.deleteCollection(databaseName , collectionName);
                        if(result) {
                            Message message = new Message();
                            message.setFunction("DeleteCollection");
                            String[] params = new String[2];
                            params[0] = databaseName;
                            params[1] = collectionName;
                            message.setParams(params);
                            thisNode.notifyObservers(message);
                        }
                        toClient.writeObject(result);
                        selection = fromClient.readInt();
                        break;
                    }
                    case 8 :{
                        //delete database
                        String databaseName = (String) fromClient.readObject();

                        boolean result = jsonFunctions.deleteDatabase(databaseName);
                        if(result) {
                            Message message = new Message();
                            String[] params = new String[1];
                            message.setFunction("DeleteDatabase");
                            params[0] = databaseName;
                            message.setParams(params);
                            thisNode.notifyObservers(message);
                        }
                        toClient.writeObject(result);

                        selection = fromClient.readInt();
                        break;

                    }


                }
                if(selection == 0 ) {

                  client.close();
                    break;

                }
            }
            if(identity.equals("Client")){
                thisNode.setNumberOfConnectedUsers(thisNode.getNumberOfConnectedUsers()-1);
                nodesDaoUser.updateNodeStatus(thisNode);
            }
            System.out.println("Service is Over");
        } catch (IOException e ){
            System.out.println("IOException");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException");
            throw new RuntimeException(e);
        }  catch (ParseException e) {
            System.out.println("ParseException");
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }
}
