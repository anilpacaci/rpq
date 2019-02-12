package ca.uwaterloo.cs.streamingrpq.input;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Yago2sInMemoryTSVStream {


    FileReader fileStream;
    BufferedReader bufferedReader;

    int[] source;
    int[] target;
    int[] edge;

    ScheduledExecutorService executor;

    Integer counter = new Integer(0);
    Integer index = 0;

    public boolean isOpen() {
        return false;
    }

    public void open(String filename) throws FileNotFoundException, IOException {
        fileStream = new FileReader(filename);
        bufferedReader = new BufferedReader(fileStream, 1024*1024);

        source = new int[200000000];
        target = new int[200000000];
        edge = new int[200000000];

        Runnable counterRunnable = new Runnable() {
            private int seconds = 0;

            @Override
            public void run() {
                System.out.println("Second " + ++seconds + " : " + counter);
                counter = 0;
            }
        };

        String line = null;
        InputTuple tuple = null;

        long startTime = System.currentTimeMillis();
        while((line = bufferedReader.readLine()) != null) {
            String[] splitResults = Iterables.toArray(Splitter.on('\t').split(line), String.class);
            if(splitResults.length == 3) {
                source[counter] = splitResults[0].hashCode();
                target[counter] = splitResults[2].hashCode();
                edge[counter] = splitResults[1].hashCode();

                counter++;

                if(counter % 10000000 == 0) {
                    System.out.println("10M in " + (System.currentTimeMillis() - startTime)/1000 + " s");
                    startTime = System.currentTimeMillis();
                }
            }
        }

        System.out.println("Input file has been buffered");

        executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(counterRunnable, 0, 1, TimeUnit.SECONDS);

    }

    public void close() throws IOException {
        bufferedReader.close();
        fileStream.close();
        executor.shutdown();
    }

    public InputTuple next() {
        if(counter == 0) {
            return null;
        }
        counter--;
        InputTuple tuple = new InputTuple(source[counter], target[counter], edge[counter]);
        index++;
        return tuple;
    }
}
