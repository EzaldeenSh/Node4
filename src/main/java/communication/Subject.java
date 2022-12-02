package communication;

import org.json.simple.parser.ParseException;

import java.io.IOException;

public interface Subject {
    public void registerObservers() throws IOException, ParseException;
    public void unregister();

    public void notifyObservers(Message message);
}
