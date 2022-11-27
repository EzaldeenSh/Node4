package data;

import json.JSONFunctions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UsersDaoUser implements UsersDao{
    private final JSONFunctions jsonFunctions;
    public UsersDaoUser() {
        jsonFunctions = JSONFunctions.getInstance();
    }

    @Override
    public List<User> findAll() throws IOException, ParseException {
        List<User> users = new ArrayList<>();
        JSONArray usersArray = jsonFunctions.readCollection("admin" , "users");
        for(Object object : usersArray){
            JSONObject currentJson = (JSONObject) object;
            User currentUser = new User( currentJson.get("username").toString(), currentJson.get("password").toString());
            users.add(currentUser);
        }

        return users;
    }

    @Override
    public boolean insertUser(String username, String password) throws IOException, ParseException {
        JSONObject userJson = new JSONObject();
        userJson.put("username" , username);
        userJson.put("password" , password);
        return jsonFunctions.writeDocument("users" , "usersInfo" , userJson);
    }

    @Override
    public boolean exists(User user) throws IOException, ParseException {
        List<User> allUsers = findAll();
        for(User current: allUsers){
            if (current.getUsername().equals(user.getUsername()) && current.getPassword().equals(user.getPassword()))
                return true;
        }
        return false;

    }
}
