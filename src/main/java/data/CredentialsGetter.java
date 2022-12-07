package data;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

public class CredentialsGetter {

    public Credentials requestCredentials(){
        try {
            Socket socket = new Socket("localhost" , 8000);
            ObjectInputStream fromBootsStrap = new ObjectInputStream(socket.getInputStream());
            Credentials myCredentials = (Credentials) fromBootsStrap.readObject();
            socket.close();
            return myCredentials;
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }

    }
}
