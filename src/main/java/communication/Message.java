package communication;

import java.io.Serializable;
import java.util.Arrays;

public class Message implements Serializable {
    private String function;
    private String[] params;

    public void setParams(String ... params) {
        this.params = params;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public String getFunction() {
        return function;
    }

    public String[] getParams() {
        return params;
    }
    @Override
    public String toString() {
        return "Message{" +
                "function='" + function + '\'' +
                ", params=" + Arrays.toString(params) +
                '}';
    }

}
