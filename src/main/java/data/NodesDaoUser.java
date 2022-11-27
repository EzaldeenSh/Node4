package data;

import communication.Message;
import json.JSONFunctions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NodesDaoUser implements NodesDao {
    private final JSONFunctions jsonFunctions;
    private static NodesDaoUser instance;


    private NodesDaoUser() {
        jsonFunctions = JSONFunctions.getInstance();

    }
    public static NodesDaoUser getInstance(){
        if (instance == null){
            instance = new NodesDaoUser();
        }
        return instance;
    }

    @Override
    public boolean updateNodeStatus(Node node) {
        int nodeIndex;
        String nodeID = node.getNodeID();
        switch (nodeID) {
            case "node1":
                nodeIndex = 0;
                break;
            case "node2":
                nodeIndex = 1;
                break;
            case "node3":
                nodeIndex = 2;
                break;
            default:
                nodeIndex = 3;
                break;
        }
        JSONObject nodeObject = new JSONObject();
        nodeObject.put("nodeID" , nodeID);
        nodeObject.put("numberOfConnectedUsers" , node.getNumberOfConnectedUsers());
        nodeObject.put("portNumber" , node.getPortNumber());
        boolean result;
        try {
            result = jsonFunctions.updateObject("admin" , "nodes" , nodeIndex , nodeObject);
            if (result){
                Message message = new Message();
                String[] params = new String[4];
                params[0] = "admin";
                params[1] = "nodes";
                params[2] = String.valueOf(nodeIndex);
                params[3] = nodeObject.toJSONString();
                message.setFunction("UpdateObject");
                message.setParams(params);
                node.notifyObservers(message);
            }
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }

        return result;
    }
    @Override
    public List<Node> findAll() throws IOException, ParseException {
        List<Node> nodeList = new ArrayList<>();
        JSONArray nodesArray = jsonFunctions.readCollection("admin" , "nodes");

        for(Object object : nodesArray){
            JSONObject current = (JSONObject) object;
            Node currentNode = new Node(current.get("nodeID").toString(), Integer.parseInt(current.get("portNumber").toString()), Integer.parseInt(current.get("numberOfConnectedUsers").toString()));


            nodeList.add(currentNode);


        }

        return nodeList;
    }


    @Override
    public Node getNode(String nodeID) throws IOException, ParseException {
        Node node = null;
        List<Node> nodesList;
        nodesList = findAll();

        for(Node currentNode: nodesList){
            String currentID = currentNode.getNodeID();

            if(currentID.equals(nodeID)){
                node = currentNode;
            }
        }

        return node;
    }
}