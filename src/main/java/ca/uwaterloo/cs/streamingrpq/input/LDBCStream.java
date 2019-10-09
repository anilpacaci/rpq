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

public class LDBCStream implements TextStream{


    FileReader fileStream;
    BufferedReader bufferedReader;

    String filename;

    ScheduledExecutorService executor;

    Integer localCounter = 0;
    Integer globalCounter = 0;

    Long startTimestamp = -1L;


    public boolean isOpen() {
        return false;
    }

    public void open(String filename, int maxSize) {
        this.startTimestamp = 0L;
        open(filename);
    }

    public void open(String filename, int maxSize, long startTimestamp) {
        this.startTimestamp = startTimestamp;
        open(filename);
    }

    public void open(String filename) {
        this.filename = filename;
        try {
            fileStream = new FileReader(filename);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        bufferedReader = new BufferedReader(fileStream, 20*1024*1024);

        Runnable counterRunnable = new Runnable() {
            private int seconds = 0;

            @Override
            public void run() {
                System.out.println("Second " + ++seconds + " : " + localCounter + " / " + globalCounter);
                localCounter = 0;
            }
        };

        executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(counterRunnable, 1, 1, TimeUnit.SECONDS);
    }

    public void close() {
        try {
            bufferedReader.close();
            fileStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        executor.shutdown();
    }

    public InputTuple<Integer, Integer, String> next() {
        String line = null;
        InputTuple tuple = null;
        try {
            while((line = bufferedReader.readLine()) != null) {
                String[] splitResults = Iterables.toArray(Splitter.on('\t').trimResults().split(line), String.class);
                if(splitResults.length == 4) {
//                    tuple = new InputTuple(1,2,3);
                    tuple = new InputTuple(splitResults[0].hashCode(), splitResults[2].hashCode(), splitResults[1], Long.parseLong(splitResults[3]) - startTimestamp);
                    localCounter++;
                    globalCounter++;
//                    tuple = new InputTuple(Integer.parseInt(splitResults[0]), Integer.parseInt(splitResults[2]), splitResults[1]);
                    break;
                }
            }
        } catch (IOException e) {
            return null;
        }
        if (line == null) {
            return null;
        }

        return tuple;
    }

    public void reset() {
        close();

        open(this.filename);

        localCounter = 0;
        globalCounter = 0;
    }
}
