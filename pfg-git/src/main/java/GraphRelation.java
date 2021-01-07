import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class that serves as a data structure that "holds" a producers - consumers kafka relation for a given topic.
 *
 * @see AbstractParser
 */
public class GraphRelation {

    private final String TOPIC;
    private final Set<String> producers = new HashSet<>();
    private final Set<String> consumers = new HashSet<>();

    public GraphRelation(String topic) {
        TOPIC = topic;
    }

    public String getTOPIC() {
        return TOPIC;
    }

    public void addProducer(String producer) {
        this.producers.add(producer);
    }

    public List<String> getProducers() {
        return new ArrayList<>(producers);
    }

    public void addConsumer(String consumer) {
        this.consumers.add(consumer);
    }

    public List<String> getConsumers() {
        return new ArrayList<>(consumers);
    }
}

