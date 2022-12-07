package communication;

import org.json.simple.parser.ParseException;

import java.io.IOException;

public interface Observer {
    public void update(Message message) throws IOException, ParseException;

}
