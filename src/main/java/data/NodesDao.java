package data;

import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.List;

public interface NodesDao {
    List<Node> findAll() throws IOException, ParseException;
    boolean updateNodeStatus(Node node);
    Node getNode(String nodeID) throws IOException, ParseException;
}
