package json;

import java.lang.reflect.Type;
public class TypesFactory {
    public Type getType(String type){
        switch (type){
            case "String":
                return String.class;
            case "int" :
                return Integer.class;
            case "char":
                return Character.class;
            case "boolean":
                return Boolean.class;
            case "long":
                return Long.class;
            case "double":
                return Double.class;
            case "float":
                return Float.class;
        }
        return null;
    }
}
