package json;

import org.json.simple.JSONObject;

import java.lang.reflect.Type;

public class ObjectValidator {
    public boolean validateObject(JSONObject object, JSONObject schema){
        if(object.size() != schema.size())
            return false;
        else {
            TypesFactory factory = new TypesFactory();
            for(Object keyString : object.keySet()){
                String schemaTypeString  = (String) schema.get(keyString);
                Type schemaType = factory.getType(schemaTypeString);
                Type objectType = object.get(keyString).getClass();
                if(objectType.equals(Integer.class) || objectType.equals(Long.class))
                {
                    if(intChecker(object.get(keyString).getClass() , schemaType))
                        continue;
                } else if(objectType.equals(Float.class) || objectType.equals(Double.class)){
                    if(doubleChecker(object.get(keyString).getClass() , schemaType))
                        continue;
                }
                if(!schemaType.equals(objectType))
                    return false;
            }
        }

        return true;
    }
    private boolean intChecker(Type objectType, Type schemaType){
        return (objectType.equals(Integer.class) || (objectType.equals(Long.class))) && (schemaType.equals(Long.class) || schemaType.equals(Integer.class));
    }
    private boolean doubleChecker(Type objectType , Type schemaType){
        return objectType.equals(Float.class) || (objectType.equals(Double.class)) && (schemaType.equals(Double.class) || schemaType.equals(Float.class));

    }
}
