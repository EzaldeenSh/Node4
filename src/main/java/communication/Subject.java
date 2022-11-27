package communication;

public interface Subject {
    public void registerObservers();
    public void unregister();

    public void notifyObservers(Message message);
}
