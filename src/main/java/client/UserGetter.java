package client;

import data.User;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class UserGetter {
    private final ObjectOutputStream toServer;
    private final ObjectInputStream fromServer;
    public UserGetter(ObjectOutputStream toServer, ObjectInputStream fromServer){
        this.toServer = toServer;
        this.fromServer = fromServer;

    }
    public User requestUser() throws IOException, ClassNotFoundException {
        toServer.writeObject("User");
        toServer.flush();
        User user = (User) fromServer.readObject();
        System.out.println(user.toString());
        return user;
    }
}
