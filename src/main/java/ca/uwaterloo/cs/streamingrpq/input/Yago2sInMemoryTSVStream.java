package ca.uwaterloo.cs.streamingrpq.input;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Yago2sInMemoryTSVStream {

    public static final Integer MAX_STREAM_SIZE = 200000000;


    FileReader fileStream;
    BufferedReader bufferedReader;

    int[] source;
    int[] target;
    int[] edge;

    ScheduledExecutorService executor;

    Integer bufferPointer = 0;
    Integer bufferSize = 0;
    Integer counter = 0;


    public boolean isOpen() {
        return false;
    }

    public void open(String filename) throws FileNotFoundException, IOException {
        open(filename, MAX_STREAM_SIZE);
    }

    public void open(String filename, Integer maxSize) throws FileNotFoundException, IOException {
        fileStream = new FileReader(filename);
        bufferedReader = new BufferedReader(fileStream, 1024*1024);

        source = new int[maxSize];
        target = new int[maxSize];
        edge = new int[maxSize];

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
                source[bufferSize] = splitResults[0].hashCode();
                target[bufferSize] = splitResults[2].hashCode();
                edge[bufferSize] = splitResults[1].hashCode();

                bufferSize++;

                if(bufferSize % 10000000 == 0) {
                    System.out.println((bufferSize / 10000000) + " in " + (System.currentTimeMillis() - startTime)/1000 + " s");
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

    /**
     * resets the index pointers so that buffered stream can be consumed again
     */
    public void reset() {
        // just set buffer pointer to the top of the buffer
        bufferPointer = 0;
    }

    public InputTuple next() {
        if(bufferPointer == bufferSize - 1) {
            return null;
        }
        bufferPointer++;
        InputTuple tuple = new InputTuple(source[bufferPointer], target[bufferPointer], edge[bufferPointer]);
        counter++;
        return tuple;
    }
}
