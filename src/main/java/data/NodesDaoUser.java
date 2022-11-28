package data;

import communication.Message;
import json.JSONFunctions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.security.InvalidParameterException;

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
    public JSONArray findAll() throws IOException, ParseException {
        JSONArray nodesArray = jsonFunctions.readCollection("admin" , "nodes");
        return nodesArray;
    }

    @Override
    public int getOwnerPort(String nodeID) throws IOException, ParseException {
       JSONArray nodesArray  = findAll();
       for(Object nodeObject : nodesArray){
           JSONObject currentObject  = (JSONObject) nodeObject;
           String currentObjectID = currentObject.get("nodeID").toString();
           if(currentObjectID.equals(nodeID)){
               return Integer.parseInt(currentObject.get("portNumber").toString());
           }
       }
        throw new InvalidParameterException("Invalid nodeID");
    }
}