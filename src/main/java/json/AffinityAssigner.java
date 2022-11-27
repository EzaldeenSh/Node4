package json;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public class AffinityAssigner {
    private static int counter =1;

    public void assignAffinityForAll() throws IOException {
        PathsHandler pathsHandler = new PathsHandler();
        String basePath = pathsHandler.getBasePath();
        File baseFile = new File(basePath);
        sortFilesInside(baseFile);
        counter = 1;

        for(File database : Objects.requireNonNull(baseFile.listFiles())){
        sortFilesInside(database);
        for (File collection : Objects.requireNonNull(database.listFiles())){
            sortFilesInside(collection);
            File affinityFile = new File(pathsHandler.getAffinityPath(database.getName() , collection.getName()));
            assignAffinity(affinityFile);
        }
        }

    }
    private void sortFilesInside(File file){
        File[]files = file.listFiles();
        assert files != null;
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
    }
    public void assignAffinity(File affinityFile) throws IOException {
        FileWriter fileWriter = new FileWriter(affinityFile);
        fileWriter.write("node" + counter);
        counter = (counter%4) + 1;
        fileWriter.close();

    }
}
