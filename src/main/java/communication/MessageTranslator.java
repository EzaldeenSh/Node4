package communication;

public final class MessageTranslator {
    public static String getFunction(Message message){
        return message.getFunction();
    }
    public static String[] getParams(Message message){
        return message.getParams();
    }
}
