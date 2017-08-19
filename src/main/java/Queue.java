import java.util.concurrent.ArrayBlockingQueue;

public class Queue {
    private ArrayBlockingQueue<String> queue;
    public String take(){
        //taking first element of queue
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }
    public boolean add(String newUrl){
        return queue.add(newUrl);
    }
    public Queue(){
        queue = new ArrayBlockingQueue<String>(10000);
    }
}
