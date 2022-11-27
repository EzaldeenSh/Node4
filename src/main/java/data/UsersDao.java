package data;

import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.List;

public interface UsersDao {
    List<User> findAll() throws IOException, ParseException;
    boolean insertUser(String username, String password) throws IOException, ParseException;
    boolean exists(User user) throws IOException, ParseException;
}
