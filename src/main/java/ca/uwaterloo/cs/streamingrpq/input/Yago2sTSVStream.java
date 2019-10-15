package ca.uwaterloo.cs.streamingrpq.input;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Yago2sTSVStream implements TextStream{


    FileReader fileStream;
    BufferedReader bufferedReader;

    String filename;

    ScheduledExecutorService executor;

    Integer localCounter = 0;
    Integer globalCounter = 0;

    private String splitResults[];

    InputTuple tuple = null;


    public boolean isOpen() {
        return false;
    }

    public void open(String filename, int maxSize) {
        open(filename);
    }

    @Override
    public void open(String filename, int size, long startTimestamp) {
        open(filename, size);
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

        splitResults = new String[4];

        tuple = new InputTuple(null, null, null, 0);
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
        InputTuple<Integer, Integer, String> tuple = null;
        try {
            while((line = bufferedReader.readLine()) != null) {
                Iterator<String> iterator = Splitter.on('\t').trimResults().split(line).iterator();
                int i = 0;
                for(i = 0; iterator.hasNext() && i < 3; i++) {
                    splitResults[i] = iterator.next();
                }
                // only if we fully
                if(i == 3) {
//                    tuple = new InputTuple(1,2,3);
                    tuple.setSource(splitResults[0].hashCode());
                    tuple.setTarget(splitResults[2].hashCode());
                    tuple.setLabel(splitResults[1]);
                    tuple.setTimestamp(globalCounter);
//                    tuple = new InputTuple(Integer.parseInt(splitResults[0]), Integer.parseInt(splitResults[2]), splitResults[1]);
                    localCounter++;
                    globalCounter++;
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
