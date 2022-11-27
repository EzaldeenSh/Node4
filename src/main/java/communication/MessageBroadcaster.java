package communication;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class MessageBroadcaster {
    public void broadcastMessage(int portNumber,Message message) throws IOException {
        System.out.println("Attempting connection on port number: " + portNumber);
        Socket socket = new Socket("localhost", portNumber);
        ObjectOutputStream toServer = new ObjectOutputStream(socket.getOutputStream());
        toServer.writeObject("Node");
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        toServer.writeObject(message);
        System.out.println("Message sent!");
        socket.close();
    }
}
