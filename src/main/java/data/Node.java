package data;


import communication.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Node implements Observer, Subject {

    private List<Integer> otherNodesPorts;
    private final String NodeId;
    private int portNumber;
    private int numberOfConnectedUsers;

    public Node(String nodeId, int portNumber, int numberOfConnectedUsers) {
        NodeId = nodeId;
        this.portNumber = portNumber;
        this.numberOfConnectedUsers = numberOfConnectedUsers;
        registerObservers();

    }
    @Override
    public void registerObservers() {
        this.otherNodesPorts = new ArrayList<>();
        otherNodesPorts.add(8081);
        otherNodesPorts.add(8082);
        otherNodesPorts.add(8083);
    }
    public String getNodeID(){
    return NodeId;
    }

    public void setPortNumber(int portNumber) {
        this.portNumber = portNumber;
    }

    public void setNumberOfConnectedUsers(int numberOfConnectedClients) {
        this.numberOfConnectedUsers = numberOfConnectedClients;
    }
    public String getNodeId() {
        return NodeId;
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
                ", nodeID=" + NodeId +
                ", numberOfConnectedUsers=" + numberOfConnectedUsers +
                '}';
    }
}
