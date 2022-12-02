package data;
import communication.*;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Node implements Observer, Subject {

    private List<Integer> otherNodesPorts;
    private final String nodeID;
    private int portNumber;
    private int numberOfConnectedUsers;
    private final NodesDaoUser nodesDaoUser;

    private static Node instance;

    private Node() {
        nodeID = "node4";

        this.numberOfConnectedUsers = 0;
         nodesDaoUser =new NodesDaoUser();
        try {
            this.portNumber = nodesDaoUser.getOwnerPort(nodeID);
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }
    public static Node getInstance(){
        if (instance == null){
            instance = new Node();
        }
        return instance;
    }
    @Override
    public void registerObservers() throws IOException, ParseException {
        this.otherNodesPorts = nodesDaoUser.getAllOtherPorts();
    }

    public void setPortNumber(int portNumber) {
        this.portNumber = portNumber;
    }

    public void setNumberOfConnectedUsers(int numberOfConnectedClients) {
        this.numberOfConnectedUsers = numberOfConnectedClients;
    }
    public List<Integer> getOtherNodesPorts(){
        return otherNodesPorts;
    }
    public String getNodeID() {
        return nodeID;
    }

    public int getPortNumber() {
        return portNumber;
    }
    public int getNumberOfConnectedUsers() {
        return numberOfConnectedUsers;
    }
    @Override
    public void update(Message message) {
    new MessageExecutor().executeMessage(message);
    }

    @Override
    public void unregister() {
    this.otherNodesPorts = new ArrayList<>();
    }

    @Override
    public void notifyObservers(Message message) {
        for(Integer portNumber : otherNodesPorts){
            try {
                this.broadcastMessage(portNumber,message);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public void broadcastMessage(int portNumber,Message message) throws IOException {
        new MessageBroadcaster().broadcastMessage(portNumber , message);
    }

    @Override
    public String toString() {
        return "Node{" +

                "otherNodesPorts=" + otherNodesPorts +
                ", portNumber=" + portNumber +
                ", nodeID=" + nodeID +
                ", numberOfConnectedUsers=" + numberOfConnectedUsers +
                '}';
    }
}
