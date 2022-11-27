package server;

import data.User;
import data.UsersDaoUser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class ClientValidator {
    UsersDaoUser usersDaoUser;
    public ClientValidator(){
        usersDaoUser= new UsersDaoUser();

    }
    public boolean validateClient(User user) throws IOException, ParseException {
        return usersDaoUser.exists(user);

    }
}
