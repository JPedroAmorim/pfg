import com.sitewhere.microservice.kafka.KafkaTopicNaming;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;


public abstract class AbstractParser {

    private final String PATH_TO_DIR;
    private final Set<String> MICROSERVICES;

    public AbstractParser(String directoryPathForProject, Set<String> microservicesToBeSearched) {
        this.PATH_TO_DIR = directoryPathForProject;
        this.MICROSERVICES = microservicesToBeSearched;
    }

    /**
     * The core method of the parser. Through it, it parses the project's data in order to generate a .dot file that
     * is a visual digraph representation of the kafka dependencies between microservices.
     * <p>
     * In such graph representation, every microservice that produces to/consumes from a kafka topic is represented by
     * an edge. Naturally, the topic defines the relation between two or more microservices by being a vertex.
     * <p>
     * In this directed graph, a vertex "comes from" a kafka producer and "is pointed at" a kafka consumer.
     *
     * @param shouldLabelEdges a boolean indicating whether or not the edges in the digraph should be labeled
     *                         with the topics that are responsible for the relation itself. If true, the output
     *                         graph will be directed to the "graph_with_labels.dot" and otherwise, it'll be
     *                         directed to "graph_no_labels.dot"
     **/
    public void writeDotForDependencies(boolean shouldLabelEdges) {
        List<GraphRelation> graphRelations = generateGraphRelations();

        printGraphRelation(graphRelations);

        String outputFileName = shouldLabelEdges ? "graph_with_labels.dot" : "graph_no_labels.dot";

        try (BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileName)))) {
            output.write("digraph {\n");

            for (GraphRelation relation : graphRelations) {
                boolean noProducers = relation.getProducers().isEmpty();
                boolean noConsumers = relation.getConsumers().isEmpty();

                if ((noProducers && !noConsumers) || (!noProducers && noConsumers)) {
                    List<String> nonEmptyCollection = noProducers ? relation.getConsumers() : relation.getProducers();

                    writeSoloKafkaElement(output, nonEmptyCollection);

                } else if (!noProducers) { // Here we're sure that it is a full relation (e.g. !noConsumers is true)
                    writeFullRelation(output, relation.getProducers(), relation.getConsumers(), relation.getTOPIC(),
                            shouldLabelEdges);
                }
            }

            output.write("}");

        } catch (java.io.IOException ex) {
            handleException(ex);
        }
    }

    /**
     * Auxiliary method used for debug purposes. It prints out on the console a String representation for graphRelations
     *
     * @param graphRelations a List of the graphRelation data structured to be printed
     **/
    public void printGraphRelation(List<GraphRelation> graphRelations) {
        for (GraphRelation relation : graphRelations) {
            StringBuilder sbConsumer = new StringBuilder();
            StringBuilder sbProducer = new StringBuilder();

            for (String consumer : relation.getConsumers()) {
                sbConsumer.append(" ").append(consumer).append(" ");
            }

            for (String producer : relation.getProducers()) {
                sbProducer.append(" ").append(producer).append(" ");
            }

            System.out.println("Consumers: " + sbConsumer.toString()
                    + "---  Topic: " + relation.getTOPIC() + " ---" + " Producers:" + sbProducer.toString());
        }
    }

    /**
     * Auxiliary method that writes a full relation (e.g. producer - consumer) onto the .dot file that represents the
     * graph.
     *
     * @param output           the BufferedWriter responsible for writing the relation
     * @param producers        a List containing the producers for a given relation
     * @param consumers        a List containing the consumers for a given relation
     * @param topic            a String containing the topic of the relation
     * @param shouldLabelEdges a boolean indicating if the edges should be labeled with the relation's topic
     * @throws java.io.IOException due to the buffered writer write() call
     **/
    private void writeFullRelation(BufferedWriter output, List<String> producers, List<String> consumers, String topic,
                                   boolean shouldLabelEdges) throws java.io.IOException {
        String topicLabel = shouldLabelEdges ? "[label=\"" + topic + "\"]" : "";

        for (String producer : producers) {
            for (String consumer : consumers) {
                output.write(producer.replace(" ", "") + " -> " +
                        consumer.replace(" ", "") + topicLabel + ";\n");
            }
        }
    }

    /**
     * Auxiliary method that writes a relation with no edges (e.g. a kafka producer producing to a topic no other kafka
     * class consumes).
     *
     * @param output        the BufferedWriter responsible for writing the relation
     * @param kafkaElements a List of either kafka producers or consumers that produce/write to a given topic
     * @throws java.io.IOException due to the buffered writer write() call
     **/
    private void writeSoloKafkaElement(BufferedWriter output, List<String> kafkaElements) throws java.io.IOException {
        for (String kafkaElement : kafkaElements) {
            output.write(kafkaElement.replace(" ", "") + ";\n");
        }
    }

    /**
     * Generates a GraphRelation.class list, consisting of the dependency relations between microservices. This
     * GraphRelation.class is a auxiliary data structure used in order to generate the .dot file.
     *
     * @see GraphRelation
     * @return a List of GraphRelation.class
     **/
    private List<GraphRelation> generateGraphRelations() {
        List<GraphRelation> graphRelations = new ArrayList<>();

        try {
            graphRelations = initGraphRelation();

            for (String microservice : MICROSERVICES) {
                generateMicroserviceRelation(microservice, graphRelations);
            }

        } catch (IllegalAccessException | NullPointerException | ClassNotFoundException ex) {
            handleException(ex);
        }

        return graphRelations;
    }

    /**
     * Initializes an empty (edge-wise) GraphRelation.class list with the topics provided by the KafkaTopicNaming.class.
     *
     * @throws IllegalAccessException Even though in architectures 2.0 and 3.0 this exception isn't thrown, there is no
     *                                guarantee that in previous or further architectures this will happen.
     * @see GraphRelation
     * @see com.sitewhere.microservice.kafka.KafkaTopicNaming
     * @return an initialized (with only the topics) GraphRelation.class list
     **/
    private List<GraphRelation> initGraphRelation() throws IllegalAccessException {
        List<GraphRelation> emptyGraphRelations = new ArrayList<>();

        Class<KafkaTopicNaming> clazz = KafkaTopicNaming.class;
        KafkaTopicNaming naming = new KafkaTopicNaming();
        Field[] fields = clazz.getDeclaredFields();

        for (Field field : fields) {
            if (field.getType() == String.class && field.getName().contains("TOPIC")) {
                field.setAccessible(true);
                Object value = field.get(naming);
                String valueString = value.toString();
                emptyGraphRelations.add(new GraphRelation(valueString));
            }
        }

        return emptyGraphRelations;
    }

    /**
     * Helper method that handles a generic exception.
     *
     * @param ex exception handled
     **/
    private void handleException(Exception ex) {
        System.err.println("Exception caught with message: " + ex.getMessage());
        System.err.println("Stack trace associated: ");
        System.err.println(Arrays.toString(ex.getStackTrace()));
    }

    /**
     * Method that for a given microservice, creates its corresponding graph relation within graphRelations.
     *
     * @param microservice   microservice whose relations will be built
     * @param graphRelations a List of the GraphRelation data structure
     * @throws ClassNotFoundException rethrows from the getClassesInKafkaDirectories method
     **/
    private void generateMicroserviceRelation(String microservice, List<GraphRelation> graphRelations)
            throws ClassNotFoundException {
        String microserviceDirectory = locateMicroserviceDirectory(microservice);
        String microserviceSourceDirectory = microserviceDirectory + "/src/main/java";

        Set<String> kafkaDirectories = new HashSet<>();
        locateKafkaDirectories(microserviceSourceDirectory, kafkaDirectories);

        Set<String> kafkaClasses = getClassesInKafkaDirectories(kafkaDirectories);

        checkKafkaRoleAndCreateVertex(kafkaClasses, graphRelations, microservice);
    }

    /**
     * For each class name within the kafkaClasses parameter, this method determines if such classes is either a kafka
     * consumer or a kafka producer and then create its owning microservice vertex on graphRelations.
     *
     * @see GraphRelation
     * @param kafkaClasses   A set of String containing all kafka classes names
     * @param graphRelations a List of the GraphRelation data structure
     * @param microservice   owning microservice for the set of kafka classes
     * @throws ClassNotFoundException if the given class String within kafkaClasses cannot be found
     **/
    private void checkKafkaRoleAndCreateVertex(Set<String> kafkaClasses, List<GraphRelation> graphRelations,
                                               String microservice)
            throws ClassNotFoundException {

        for (String clazzName : kafkaClasses) {
            if (isConsumer(Class.forName(clazzName))) {
                createConsumerVertex(graphRelations, clazzName, microservice);
            } else if (isProducer(Class.forName(clazzName))) {
                createProducerVertex(graphRelations, clazzName, microservice);
            }
        }
    }

    /**
     * Creates a consumer vertex within graphRelations for a given microservice if the consumerName parameter matches an
     * existing topic.
     *
     * @param graphRelations a List of the graphRelation data structure
     * @param consumerName   the class name for a kafka consumer
     * @param microservice   owning microservice for the consumer
     * @see GraphRelation
     **/
    private void createConsumerVertex(List<GraphRelation> graphRelations, String consumerName,
                                      String microservice) {
        for (GraphRelation relation : graphRelations) {
            if (nameMatchingTopic(relation.getTOPIC(), consumerName, "Consumer")) {
                relation.addConsumer(microservice);
            }
        }
    }

    /**
     * Creates a producer vertex within graphRelations for a given microservice if the producerName parameter matches an
     * existing topic.
     *
     * @param graphRelations a List of the graphRelation data structure
     * @param producerName   the class name for a kafka producer
     * @param microservice   owning microservice for the producer
     * @see GraphRelation
     **/
    private void createProducerVertex(List<GraphRelation> graphRelations, String producerName,
                                      String microservice) {
        for (GraphRelation relation : graphRelations) {
            if (nameMatchingTopic(relation.getTOPIC(), producerName, "Producer")) {
                relation.addProducer(microservice);
            }
        }
    }

    /**
     * Defines if a certain kafka class (e.g. a consumer) has a lexicographical relation with a given topic.
     *
     * @param topic      the topic to be compared
     * @param kafkaClass the kafka element class that'll be compared
     * @param suffix     String indicating if the kafka element being compared is a consumer or a producer
     * @return a boolean indicating if there's a correlation
     **/
    public static boolean nameMatchingTopic(String topic, String kafkaClass, String suffix) {
        if (!kafkaClass.contains(suffix)) {
            return false;
        }

        String meaningfulSubstringForConsumer = kafkaClass.substring(kafkaClass.indexOf("kafka") + 6,
                kafkaClass.lastIndexOf(suffix));

        String[] meaningfulSubstringsForConsumer = meaningfulSubstringForConsumer.split("(?=\\p{Upper})");

        int containsCount = 0;

        for (String substring : meaningfulSubstringsForConsumer) {
            if (topic.contains(substring.toLowerCase())) {
                containsCount++;
            }
        }

        double correctnessPercentage = (double) containsCount / (double) meaningfulSubstringsForConsumer.length;

        return correctnessPercentage >= (double) 3 / (double) 4;
    }

    /**
     * Creates a proper kafka class name from a given file path.
     *
     * @param filePath String representing the file path that will be converted on a class name
     * @return a String representing a class that can be used as an argument for getClass (reflection)
     **/
    protected String generateClassString(String filePath) {
        return filePath.substring(filePath.lastIndexOf("com"), filePath.lastIndexOf(".java"))
                .replace("/", ".");
    }

    /**
     * Method that returns the String representation of the absolute directory path that contains the target microservice
     * or an empty String if such directory doesn't exist.
     *
     * @param target the microservice whose directory is searched
     * @return a String containing the absolute path for a given target microservice of an empty String if the directory
     * isn't found
     **/
    private String locateMicroserviceDirectory(String target) {
        File directory = new File(PATH_TO_DIR);
        File[] fileList = directory.listFiles();

        if (fileList == null) {
            throw new NullPointerException();
        }

        for (File file : fileList) {
            if (compare(file.getAbsolutePath(), target)) {
                return file.getAbsolutePath();
            }
        }

        return "";
    }

    /**
     * Method that converts all files within a kafka directory to their proper class names. Since the structure of a
     * kafka directory may differ between architectural versions (e.g. the kafka directories have sub directories, for
     * instance), is up for the concrete implementations of this classes to implement this method.
     *
     * @param kafkaDirectories a set containing all kafka directories for a given microservice
     * @return a set containing all kafka-related class names within the given kafka directories
     **/
    protected abstract Set<String> getClassesInKafkaDirectories(Set<String> kafkaDirectories);

    /**
     * Method that locates all Kafka directories from a given microservice directory.
     *
     * @param microserviceSourceDirectory String representing the microservice source directory that should be searched
     *                                    for kafka sub directories.
     * @param kafkaDirectories            a set containing all the kafka directories found throughout the recursion
     **/
    private void locateKafkaDirectories(String microserviceSourceDirectory, Set<String> kafkaDirectories) {
        File directory = new File(microserviceSourceDirectory);
        File[] fileList = directory.listFiles();

        if (fileList == null) {
            throw new NullPointerException();
        }

        for (File file : fileList) {
            if (file.isDirectory()) {
                // We're not interested in interfaces, so we remove entries that contain "spi"
                if (compare(file.getAbsolutePath(), "kafka") && !file.getAbsolutePath().contains("spi")) {
                    kafkaDirectories.add(file.getAbsolutePath());
                } else {
                    locateKafkaDirectories(file.getAbsolutePath(), kafkaDirectories);
                }
            }
        }
    }

    /**
     * Auxiliary method used in order to determine if a given file path has a lexicographical relation with a
     * given target.
     *
     * @param filePath a String that represents the absolute path of the file that will be compared
     * @param target   a String that represents some target value (e.g. a microservice name)
     * @return a boolean value indicating if there's a relation or not between the parameters
     **/
    private static boolean compare(String filePath, String target) {
        String meaningfulSubstring = filePath.substring(filePath.lastIndexOf("sitewhere/") + 10);

        meaningfulSubstring = meaningfulSubstring.replace("-", " ");

        return meaningfulSubstring.toLowerCase().contains(target.toLowerCase());
    }

    /**
     * Method that uses reflection to determine whether or not a certain Class is a Kafka Producer.
     *
     * @param subjectClass the class that will be inspected
     * @return a boolean value indicating if the given class is a Kafka Producer
     **/
    private boolean isProducer(Class<?> subjectClass) {
        // Check the fields within the subject and see if any of them is an instance of KafkaProducer
        Field[] fields = subjectClass.getDeclaredFields();

        for (Field field : fields) {
            boolean isInstanceOfKafkaProducer = field.getType() == KafkaProducer.class;
            if (isInstanceOfKafkaProducer) {
                return true;
            }
        }

        // If there's a Superclass, check if it has an instance of a KafkaProducer
        if (subjectClass.getSuperclass() != null) {
            return isProducer(subjectClass.getSuperclass());
        } else { // If there's no Superclass, recursion bottoms out and should return false
            return false;
        }
    }

    /**
     * Method to determine whether or not a certain Class is a Kafka Consumer. It utilizes the same logic as the
     * isProducer method.
     *
     * @param subjectClass the class that will be inspected
     * @return a boolean value indicating if the given class is a Kafka Consumer
     **/
    private boolean isConsumer(Class<?> subjectClass) {
        Field[] fields = subjectClass.getDeclaredFields();

        for (Field field : fields) {
            boolean isInstanceOfKafkaConsumer = field.getType() == KafkaConsumer.class;
            if (isInstanceOfKafkaConsumer) {
                return true;
            }
        }

        if (subjectClass.getSuperclass() != null) {
            return isConsumer(subjectClass.getSuperclass());
        } else {
            return false;
        }
    }
}
