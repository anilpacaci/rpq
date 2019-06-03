package ca.uwaterloo.cs.streamingrpq.input;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.io.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Yago2sTSVStream {


    FileReader fileStream;
    BufferedReader bufferedReader;

    ScheduledExecutorService executor;

    Integer localCounter = 0;
    Integer globalCounter = 0;


    public boolean isOpen() {
        return false;
    }

    public void open(String filename, int maxSize) throws FileNotFoundException {
        open(filename);
    }

    public void open(String filename) throws FileNotFoundException {
        fileStream = new FileReader(filename);
        bufferedReader = new BufferedReader(fileStream, 1024*1024);

        Runnable counterRunnable = new Runnable() {
            private int seconds = 0;

            @Override
            public void run() {
                System.out.println("Second " + ++seconds + " : " + localCounter + " / " + globalCounter);
                localCounter = 0;
            }
        };

        executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(counterRunnable, 0, 1, TimeUnit.SECONDS);

    }

    public void close() throws IOException {
        bufferedReader.close();
        fileStream.close();
        executor.shutdown();
    }

    public InputTuple next() {
        String line = null;
        InputTuple tuple = null;
        try {
            while((line = bufferedReader.readLine()) != null) {
                String[] splitResults = Iterables.toArray(Splitter.on('\t').split(line), String.class);
                if(splitResults.length == 3) {
//                    tuple = new InputTuple(1,2,3);
                    tuple = new InputTuple(splitResults[0].hashCode(), splitResults[2].hashCode(), splitResults[1]);
                    break;
                }
            }
        } catch (IOException e) {
            return null;
        }
        if (line == null) {
            return null;
        }
        localCounter++;
        globalCounter++;
        return tuple;
    }

    public void reset() {
        localCounter = 0;
        globalCounter = 0;
    }
}
