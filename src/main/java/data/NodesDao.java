package data;

import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public interface NodesDao {
    JSONArray findAll() throws IOException, ParseException;
    boolean updateNodeStatus(Node node);
    int getOwnerPort(String nodeID) throws IOException, ParseException;
}
