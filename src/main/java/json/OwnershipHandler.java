package json;

import data.NodesDaoUser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

public class OwnershipHandler {
    private final PathsHandler pathsHandler;
    private final NodesDaoUser nodesDaoUser;
    public OwnershipHandler(){
        this.pathsHandler = new PathsHandler();
        this.nodesDaoUser = NodesDaoUser.getInstance();
    }
    public String getCollectionOwner(String databaseName, String collectionName){
        String affinityPath = pathsHandler.getAffinityPath(databaseName , collectionName);
        File affinityFile = new File(affinityPath);
        String owner = "";
        if(affinityFile.exists()) {
            try {
                Scanner scanner = new Scanner(affinityFile);
                owner = scanner.nextLine();
                scanner.close();
            } catch (FileNotFoundException e) {
                System.out.println("File not found");
            }
        }
        return owner;
    }
    public int getOwnerPortNumber(String databaseName , String collectionName){
        String affinityPath = pathsHandler.getAffinityPath(databaseName , collectionName);
        try {
            Scanner scanner = new Scanner(new File(affinityPath));
            String OwnerID = scanner.nextLine();
            int ownerID = nodesDaoUser.getOwnerPort(OwnerID);
            scanner.close();
            return ownerID;

        } catch (FileNotFoundException e) {
            return -1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

    }
}
