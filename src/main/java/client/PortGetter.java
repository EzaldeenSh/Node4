package client;



import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class PortGetter {
    private final ObjectOutputStream toServer;
    private final ObjectInputStream fromServer;
    public PortGetter(ObjectOutputStream toServer, ObjectInputStream fromServer){
        this.toServer = toServer;
        this.fromServer = fromServer;
    }

    public int requestPortNumber() throws IOException {

        System.out.println("connected on bootstrap for port");

        toServer.writeObject("Port");
        toServer.flush();
        int portNumber = fromServer.readInt();
        System.out.println(portNumber);
        return portNumber;
    }

}
