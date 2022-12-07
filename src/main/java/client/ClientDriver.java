package client;

import data.Credentials;
import data.CredentialsGetter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;


public class ClientDriver {
    private Socket socket;
    private ObjectOutputStream toServer;
    private ObjectInputStream fromServer;
    private boolean isConnected;
    private Credentials credentials;
    private static ClientDriver instance;
    private ClientDriver(){}

    public static ClientDriver getInstance() {
        if(instance == null)
            instance = new ClientDriver();
        return instance;
    }
    public void createCredentials(){
        this.credentials = new CredentialsGetter().requestCredentials();
    }
    public boolean login(){
        try {
            this.socket = new Socket("localhost" , credentials.getPortNumber());
            toServer = new ObjectOutputStream(socket.getOutputStream());
            fromServer = new ObjectInputStream(socket.getInputStream());
            toServer.writeObject("Client");
            toServer.writeObject(credentials.getUser().getUsername());
            toServer.writeObject(credentials.getUser().getPassword());
            isConnected = (boolean) fromServer.readObject();

            return isConnected;
        } catch (IOException | ClassNotFoundException e) {
            return false;
        }
    }
    public boolean createDatabase(String databaseName){
            if (!isConnected)
                return false;
        try {
            toServer.writeInt(1);
            toServer.writeObject(databaseName);
            toServer.flush();
            return (boolean) fromServer.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return false;
        }
    }
    public boolean createCollection(String databaseName, String collectionName, JSONObject schema){
        if (!isConnected)
            return false;
        try {
            toServer.writeInt(2);
            toServer.writeObject(databaseName);
            toServer.writeObject(collectionName);
            toServer.writeObject(schema);
            toServer.flush();
            return (boolean) fromServer.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return false;
        }
    }
    public JSONObject readObjectByIndex(String databaseName, String collectionName, int index){
        if (!isConnected)
            return new JSONObject();
        try {
            toServer.writeInt(3);
            toServer.writeObject(databaseName);
            toServer.writeObject(collectionName);
            toServer.writeObject(index);
            toServer.flush();

            return (JSONObject) fromServer.readObject();

        } catch (IOException | ClassNotFoundException e) {
            return new JSONObject();
        }
    }

    public JSONArray readCollection(String databaseName, String collectionName){
        if(!isConnected)
            return new JSONArray();
        try {
            toServer.writeInt(4);
            toServer.writeObject(databaseName);
            toServer.writeObject(collectionName);
            toServer.flush();
            return (JSONArray) fromServer.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return new JSONArray();
        }
    }

    public boolean writeObject(String databaseName, String collectionName, JSONObject jsonObject){
        if(!isConnected)
            return false;
        try {
            toServer.writeInt(5);
            toServer.writeObject(databaseName);
            toServer.writeObject(collectionName);
            toServer.writeObject(jsonObject);
            toServer.flush();
            return (boolean) fromServer.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return false;
        }

    }

    public boolean updateObjectOnIndex(String databaseName, String collectionName, int index, JSONObject newObject){
        if(!isConnected)
            return false;
        try {
            toServer.writeInt(6);
            toServer.writeObject(databaseName);
            toServer.writeObject(collectionName);
            toServer.writeInt(index);
            toServer.writeObject(newObject);
            toServer.flush();
            return (boolean) fromServer.readObject();
        } catch (IOException | ClassNotFoundException e) {
          return false;
        }

    }
    public boolean deleteCollection(String databaseName, String collectionName){
        if(!isConnected)
            return false;
        try {
            toServer.writeInt(7);
            toServer.writeObject(databaseName);
            toServer.writeObject(collectionName);
            toServer.flush();
            return (boolean) fromServer.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return false;
        }
    }

    public boolean deleteDatabase(String databaseName){
        if (!isConnected)
            return false;
        try {
            toServer.writeInt(8);
            toServer.writeObject(databaseName);
            toServer.flush();
            return (boolean) fromServer.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return false;
        }
    }

    public boolean createIndexOnAJSONProperty(String databaseName,String collectionName, String propertyName){
        if(!isConnected)
            return false;
        try {
            toServer.writeInt(9);
            toServer.writeObject(databaseName);
            toServer.writeObject(collectionName);
            toServer.writeObject(propertyName);
            toServer.flush();
            return (boolean) fromServer.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return false;
        }
    }
    public ArrayList<Long> getJSONPropertyIndexes(String databaseName, String collectionName, String propertyName, String propertyValue){
        if(!isConnected)
            return new ArrayList<>();
        try {
            toServer.writeInt(10);
            toServer.writeObject(databaseName);
            toServer.writeObject(collectionName);
            toServer.writeObject(propertyName);
            toServer.writeObject(propertyValue);
            return (ArrayList<Long>) fromServer.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return new ArrayList<>();
        }

    }
    public void logout(){
        try {
            toServer.writeInt(0);
            toServer.flush();
            socket.close();
            isConnected = false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean isConnected(){
        return isConnected;
    }
}
