package client;

import data.User;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class Client1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Socket bootstrapClient = new Socket("localhost" , 8000);
        ObjectOutputStream toBootstrap = new ObjectOutputStream(bootstrapClient.getOutputStream());
        ObjectInputStream fromBootstrap = new ObjectInputStream(bootstrapClient.getInputStream());

        PortGetter portGetter = new PortGetter(toBootstrap, fromBootstrap);
        UserGetter userGetter = new UserGetter(toBootstrap , fromBootstrap);
        User myUser = userGetter.requestUser();
        int portNumber = portGetter.requestPortNumber();
        System.out.println("Port number is: "+portNumber);
        bootstrapClient.close();
        System.out.println("bootstrapClient is closed");
        ObjectInputStream fromServer;
        ObjectOutputStream toServer;

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        try (Socket client = new Socket("localhost", portNumber)) {
            System.out.println("connected to " + portNumber);
            Scanner sc = new Scanner(System.in);
            toServer = new ObjectOutputStream(client.getOutputStream());
            fromServer = new ObjectInputStream(client.getInputStream());
            toServer.writeObject("Client");

            toServer.writeObject(myUser.getUsername());
            toServer.writeObject(myUser.getPassword());
            boolean validation = (Boolean) fromServer.readObject();
            if(validation)
            {
                System.out.println("Validated!");
            }
            System.out.println("Please select an operation: \n" +
                    "1-Create Database.\n" +
                    "2-Create Collection.\n" +
                    "3-Read Object by Index.\n" +
                    "4-Read Collection.\n" +
                    "5-Write Collection.\n" +
                    "6-Update Object By Index. \n" +
                    "7-Delete Collection.\n" +
                    "8-Delete Database.\n" +
                    "0-Exit.");
            int selection = sc.nextInt();
            toServer.writeInt(selection);
            toServer.flush();

            while(selection!=0){
                switch (selection){
                      case 1:{

                        System.out.println("Please enter the name of your database: ");

                        String databaseName = sc.next();
                        toServer.writeObject(databaseName);

                        boolean result = (Boolean) fromServer.readObject();
                        if(result){
                            System.out.println("Database Created!");
                        } else
                            System.out.println("Database not Created!");
                          System.out.println("Please select an operation: \n" +
                                  "1-Create Database.\n" +
                                  "2-Create Collection.\n" +
                                  "3-Read Object by Index.\n" +
                                  "4-Read Collection.\n" +
                                  "5-Write Collection.\n" +
                                  "6-Update Object By Index. \n" +
                                  "7-Delete Collection.\n" +
                                  "8-Delete Database.\n" +
                                  "0-Exit.");

                        selection = Integer.parseInt(sc.next());
                        toServer.writeInt(selection);
                            toServer.flush();
                         break;
                 }
                 case 2:{
                     System.out.println("Please enter the name of the database: ");
                     String databaseName = sc.next();
                     System.out.println("Please enter the name of the collection: ");
                     String collectionName = sc.next();
                     System.out.println("Please enter your json schema: ");
                     String schema = sc.next();
                     JSONParser jsonParser = new JSONParser();
                     JSONObject jsonSchema = (JSONObject) jsonParser.parse(schema);

                     toServer.writeObject(databaseName);
                     toServer.writeObject(collectionName);
                     toServer.writeObject(jsonSchema);

                     boolean result = (Boolean)fromServer.readObject();
                     if(result)
                         System.out.println("Collection created!");
                     else
                         System.out.println("Collection not created!");

                     System.out.println("Please select an operation: \n" +
                             "1-Create Database.\n" +
                             "2-Create Collection.\n" +
                             "3-Read Object by Index.\n" +
                             "4-Read Collection.\n" +
                             "5-Write Collection.\n" +
                             "6-Update Object By Index. \n" +
                             "7-Delete Collection.\n" +
                             "8-Delete Database.\n" +
                             "0-Exit.");
                     selection = Integer.parseInt(sc.next());
                     toServer.writeInt(selection);
                     toServer.flush();
                     break;
                 }
                 case 3:{

                     System.out.println("Please enter the name of the database: ");
                     String databaseName = sc.next();
                     System.out.println("Please enter the name of the collection: ");
                     String collectionName = sc.next();
                     System.out.println("Please enter the index of the wanted object");
                     int index = Integer.parseInt(sc.next());

                     toServer.writeObject(databaseName);
                     toServer.writeObject(collectionName);
                     toServer.writeObject(index);

                     JSONObject jsonObject = (JSONObject) fromServer.readObject();
                     System.out.println(jsonObject+"\n");

                     System.out.println("Please select an operation: \n" +
                             "1-Create Database.\n" +
                             "2-Create Collection.\n" +
                             "3-Read Object by Index.\n" +
                             "4-Read Collection.\n" +
                             "5-Write Collection.\n" +
                             "6-Update Object By Index. \n" +
                             "7-Delete Collection.\n" +
                             "8-Delete Database.\n" +
                             "0-Exit.");

                     selection = Integer.parseInt(sc.next());
                     toServer.writeInt(selection);
                     toServer.flush();
                     break;
                 }
                 case 4:{

                     System.out.println("Please enter the name of the database: ");
                     String databaseName = sc.next();
                     System.out.println("Please enter the name of the collection: ");
                     String collectionName = sc.next();

                     toServer.writeObject(databaseName);
                     toServer.writeObject(collectionName);

                     JSONArray jsonArray = (JSONArray) fromServer.readObject();

                     for(Object o : jsonArray){
                         System.out.println(o);
                     }

                     System.out.println("Please select an operation: \n" +
                             "1-Create Database.\n" +
                             "2-Create Collection.\n" +
                             "3-Read Object by Index.\n" +
                             "4-Read Collection.\n" +
                             "5-Write Collection.\n" +
                             "6-Update Object By Index. \n" +
                             "7-Delete Collection.\n" +
                             "8-Delete Database.\n" +
                             "0-Exit.");

                     selection = Integer.parseInt(sc.next());
                     toServer.writeInt(selection);
                     toServer.flush();
                     break;
                 }
                 case 5:{
                     System.out.println("Please enter the name of the database: ");
                     String databaseName = sc.next();
                     System.out.println("Please enter the name of the collection: ");
                     String collectionName = sc.next();
                     JSONObject jsonObject = new JSONObject();
                     jsonObject.put("nodeID" , "node4" );
                     jsonObject.put("numberOfConnectedUsers" , 0);
                     jsonObject.put("portNumber" , 8084);
                     jsonObject.put("isActive" , true);
                     toServer.writeObject(databaseName);
                     toServer.writeObject(collectionName);
                     toServer.writeObject(jsonObject);
                     boolean result = (Boolean) fromServer.readObject();
                     if(result)
                         System.out.println("Object Written");
                     else
                         System.out.println("Object not Written!");
                     System.out.println("Please select an operation: \n" +
                             "1-Create Database.\n" +
                             "2-Create Collection.\n" +
                             "3-Read Object by Index.\n" +
                             "4-Read Collection.\n" +
                             "5-Write Collection.\n" +
                             "6-Update Object By Index. \n" +
                             "7-Delete Collection.\n" +
                             "8-Delete Database.\n" +
                             "0-Exit.");


                     selection = Integer.parseInt(sc.next());
                     toServer.writeInt(selection);
                     toServer.flush();
                     break;
                 }
                 case 6:{
                     System.out.println("Please enter the name of the database: ");
                     String databaseName = sc.next();
                     System.out.println("Please enter the name of the collection: ");
                     String collectionName = sc.next();
                     System.out.println("Please enter the index");
                     int index = sc.nextInt();

                     JSONObject jsonObject = new JSONObject();
                     jsonObject.put("name" , "Ezz" );
                     toServer.writeObject(databaseName);
                     toServer.writeObject(collectionName);
                     toServer.writeInt(index);
                     toServer.writeObject(jsonObject);
                     boolean result = (Boolean) fromServer.readObject();
                     if(result)
                         System.out.println("Object Updated");
                     else
                         System.out.println("Object not Updated!");
                     System.out.println("Please select an operation: \n" +
                             "1-Create Database.\n" +
                             "2-Create Collection.\n" +
                             "3-Read Object by Index.\n" +
                             "4-Read Collection.\n" +
                             "5-Write Collection.\n" +
                             "6-Update Object By Index. \n" +
                             "7-Delete Collection.\n" +
                             "8-Delete Database.\n" +
                             "0-Exit.");


                     selection = Integer.parseInt(sc.next());
                     toServer.writeInt(selection);
                     toServer.flush();
                     break;
                 }
                 case 7:{
                     System.out.println("Please enter the name of the database: ");
                     String databaseName = sc.next();
                     System.out.println("Please enter the name of the collection: ");
                     String collectionName = sc.next();
                     toServer.writeObject(databaseName);
                     toServer.writeObject(collectionName);

                     boolean result = (Boolean)fromServer.readObject();
                     if(result) System.out.println("Collection Deleted!");
                     else System.out.println("Collection not Deleted!");


                     System.out.println("Please select an operation: \n" +
                             "1-Create Database.\n" +
                             "2-Create Collection.\n" +
                             "3-Read Object by Index.\n" +
                             "4-Read Collection.\n" +
                             "5-Write Collection.\n" +
                             "6-Update Object By Index. \n" +
                             "7-Delete Collection.\n" +
                             "8-Delete Database.\n" +
                             "0-Exit.");


                     selection = Integer.parseInt(sc.next());
                     toServer.writeInt(selection);
                     toServer.flush();
                     break;
                 }
                 case 8:{
                     System.out.println("Please enter the name of your database: ");

                     String databaseName = sc.next();
                     toServer.writeObject(databaseName);

                     boolean result = (Boolean) fromServer.readObject();
                     if(result){
                         System.out.println("Database Deleted!");
                     } else
                         System.out.println("Database not Deleted!");
                     System.out.println("Please select an operation: \n" +
                             "1-Create Database.\n" +
                             "2-Create Collection.\n" +
                             "3-Read Object by Index.\n" +
                             "4-Read Collection.\n" +
                             "5-Write Collection.\n" +
                             "6-Update Object By Index. \n" +
                             "7-Delete Collection.\n" +
                             "8-Delete Database.\n" +
                             "0-Exit.");

                     selection = Integer.parseInt(sc.next());
                     toServer.writeInt(selection);
                     toServer.flush();
                     break;
                 }
                     default:{
                         System.out.println("Invalid Selection");
                         selection = 0;
                         break;
                     }

                }
            }
            Thread.sleep(100);
            client.close();
            System.out.println("Client service is over");


        } catch (UnknownHostException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.out.println("IOException");
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException");
            throw new RuntimeException(e);
        } catch (ParseException e) {
            System.out.println("ParseException");
            throw new RuntimeException(e);
        }


    }

}
