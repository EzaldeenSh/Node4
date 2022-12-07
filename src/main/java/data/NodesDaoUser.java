package data;

import communication.Message;
import json.JSONFunctions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;


public class NodesDaoUser implements NodesDao {
    private final JSONFunctions jsonFunctions;


    public NodesDaoUser() {
        jsonFunctions = new JSONFunctions();
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
            case"node4":
                nodeIndex = 3;
                break;
            default:
                nodeIndex =-1;
                break;
        }
        JSONObject nodeObject = new JSONObject();
        nodeObject.put("nodeID" , nodeID);
        nodeObject.put("numberOfConnectedUsers" , node.getNumberOfConnectedUsers());
        nodeObject.put("portNumber" , node.getPortNumber());
        nodeObject.put("isActive" , true);

        boolean result;
        result = jsonFunctions.updateObject("admin" , "nodesInfo" , nodeIndex , nodeObject);
        if (result){
            Message message = new Message();
            String[] params = new String[4];
            params[0] = "admin";
            params[1] = "nodesInfo";
            params[2] = String.valueOf(nodeIndex);
            params[3] = nodeObject.toJSONString();
            message.setFunction("UpdateObject");
            message.setParams(params);
            node.notifyObservers(message);
        }

        return result;
    }
    @Override
    public JSONArray findAll() throws IOException, ParseException {
        return jsonFunctions.readCollection("admin" , "nodesInfo");
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

    @Override
    public List<Integer> getAllOtherPorts() throws IOException, ParseException {
        List<Integer> otherPorts = new ArrayList<>();
        JSONArray otherNodesArray = findAll();
        Node thisNode = Node.getInstance();
        for(Object o : otherNodesArray){
            JSONObject current = (JSONObject) o;
            if(current.get("nodeID").toString().equals(thisNode.getNodeID())){
                continue;
            }
            otherPorts.add(Integer.valueOf(current.get("portNumber").toString()));
        }

        return otherPorts;
    }
}