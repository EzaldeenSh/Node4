package communication;
import data.Node;
import data.NodesDaoUser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

public class NodesHandler implements Runnable{
    private final Socket client;
    private final NodesDaoUser nodesDaoUser;
    private final ObjectInputStream fromClient;
    public NodesHandler(Socket otherNode, ObjectInputStream fromClient){
        this.client = otherNode;
        this.fromClient = fromClient;
        nodesDaoUser= NodesDaoUser.getInstance();
    }
    @Override
    public void run() {

        try {

            Message message = (Message) fromClient.readObject();
            Node thisNode = nodesDaoUser.getNode("node4");
            thisNode.update(message);
            client.close();

        } catch (IOException | ClassNotFoundException | ParseException e) {
            throw new RuntimeException(e);
        }

    }
}
