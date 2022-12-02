package communication;
import data.Node;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

public class NodesHandler implements Runnable{
    private final Socket client;
    private final ObjectInputStream fromClient;
    public NodesHandler(Socket otherNode, ObjectInputStream fromClient){
        this.client = otherNode;
        this.fromClient = fromClient;
    }
    @Override
    public void run() {

        try {

            Message message = (Message) fromClient.readObject();
            Node thisNode = Node.getInstance();
            thisNode.update(message);
            client.close();

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }
}
