import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class ConcreteParserForSecondArchitecture extends AbstractParser {

    public ConcreteParserForSecondArchitecture(String directoryPathForProject, Set<String> microservicesToBeSearched) {
        super(directoryPathForProject, microservicesToBeSearched);
    }

    @Override
    protected Set<String> getClassesInKafkaDirectories(Set<String> kafkaDirectories) {
        Set<String> kafkaClasses = new HashSet<>();

        for (String directoryPath : kafkaDirectories) {
            File directory = new File(directoryPath);
            File[] fileList = directory.listFiles();

            if (fileList == null) {
                throw new NullPointerException();
            }

            for (File file : fileList) {
                if (file.isDirectory() && file.listFiles() != null) {
                    for (File nestedFile : file.listFiles()) {
                        String classString = generateClassString(nestedFile.getAbsolutePath());
                        kafkaClasses.add(classString);
                    }
                    continue;
                }
                String classString = generateClassString(file.getAbsolutePath());
                kafkaClasses.add(classString);
            }

        }
        return kafkaClasses;
    }
}




