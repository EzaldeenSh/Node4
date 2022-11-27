package json;

import org.json.simple.JSONObject;

import java.lang.reflect.Type;

public class ObjectValidator {
    public boolean validateObject(JSONObject object, JSONObject schema){
        System.out.println(schema);
        System.out.println(schema.size());


        if(object.size() != schema.size())
            return false;
        else {
            TypesFactory factory = new TypesFactory();
            for(Object keyString : object.keySet()){
                String schemaType  = (String) schema.get(keyString);
                Type type = factory.getType(schemaType);
                if(object.get(keyString).getClass().equals(Integer.class) || object.get(keyString).getClass().equals(Long.class))
                {
                    if(intChecker(object.get(keyString).getClass() , type))
                        continue;
                } else if(object.get(keyString).getClass().equals(Float.class)){
                    if(doubleChecker(object.get(keyString).getClass() , type))
                        continue;
                }
                if(!(type.toString().equals(object.get(keyString).getClass().toString())))
                    return false;
            }
        }

        return true;
    }
    boolean intChecker(Type objectType, Type schemaType){
        if ((objectType.equals(Integer.class) || (objectType.equals(Long.class))) && ( schemaType.equals(Long.class) || schemaType.equals(Integer.class))){
            return true;
        } return false;
    }
    boolean doubleChecker(Type objectType , Type schemaType){
        if (objectType.equals(Float.class) && ( schemaType.equals(Double.class) || schemaType.equals(Float.class))){
            return true;
        } return false;

    }
}
