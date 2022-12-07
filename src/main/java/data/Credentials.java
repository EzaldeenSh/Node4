package data;

import java.io.Serializable;

public class Credentials implements Serializable {
    private final User user;
    private final int portNumber;

    public Credentials(User user, int portNumber) {
        this.user = user;
        this.portNumber = portNumber;
    }

    public User getUser() {
        return user;
    }

    public int getPortNumber() {
        return portNumber;
    }
}
