package server;

import communication.NodesHandler;
import data.Node;
import json.AffinityAssigner;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    public static void main(String[] args) {
        Node thisNode = Node.getInstance();
        try {
            thisNode.registerObservers();
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }

        try {
            new AffinityAssigner().assignAffinityForAll();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            ServerSocket serverSocket = new ServerSocket(thisNode.getPortNumber());
            serverSocket.setReuseAddress(true);
            System.out.println("Server Started!");

            while(true){
                Socket client = serverSocket.accept();
                System.out.println("Client Accepted");
                ObjectOutputStream toClient = new ObjectOutputStream(client.getOutputStream());
                ObjectInputStream fromClient = new ObjectInputStream(client.getInputStream());
                String identity = (String)fromClient.readObject();
                System.out.println(identity);
                if(identity.equals("Node")){
                    NodesHandler nodesHandler = new NodesHandler(client , fromClient);
                    new Thread(nodesHandler).start();
                }
                else if(identity.equals("Client") || identity.equals("ClientNode")){
                    ClientHandler clientHandler = new ClientHandler(fromClient , toClient, identity, client);
                    new Thread(clientHandler).start();
                }
                else {
                    System.out.println("Invalid connection attempt");
                }
            }


        } catch (IOException e) {

            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            System.out.println("invalid connection attempt");
        }

    }
}
