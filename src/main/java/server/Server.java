package server;

import communication.NodesHandler;
import json.AffinityAssigner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    public static void main(String[] args) {
        try {
            new AffinityAssigner().assignAffinityForAll();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            ServerSocket serverSocket = new ServerSocket(8084);
            serverSocket.setReuseAddress(true);
            System.out.println("server.Server Started!");

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
