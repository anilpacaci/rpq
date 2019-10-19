package ca.uwaterloo.cs.streamingrpq.input;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class LDBCStream implements TextStream{


    FileReader fileStream;
    BufferedReader bufferedReader;

    String filename;

    ScheduledExecutorService executor;

    Integer localCounter = 0;
    Integer globalCounter = 0;

    Integer deleteCounter = 0;

    Long startTimestamp = -1L;

    long lastTimetamp = Long.MIN_VALUE;

    InputTuple tuple = null;

    private String splitResults[];

    Queue<String> deletionBuffer = new ArrayDeque<>();
    int deletionPercentage = 0;


    public boolean isOpen() {
        return false;
    }

    public void open(String filename, int maxSize) {
        this.startTimestamp = 0L;
        open(filename);
    }

    public void open(String filename, int maxSize, long startTimestamp, int deletionPercentage) {
        this.startTimestamp = startTimestamp;
        this.deletionPercentage = deletionPercentage;
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
                System.out.println("Second " + ++seconds + " : " + localCounter + " / " + globalCounter + " -- deletes: " + deleteCounter);
                localCounter = 0;
                deleteCounter = 0;
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
        int i = 0;

        //generate negative tuple if necessary
        if(!deletionBuffer.isEmpty() && ThreadLocalRandom.current().nextInt(100) < deletionPercentage) {
            line = deletionBuffer.poll();
            Iterator<String> iterator = Splitter.on('\t').trimResults().split(line).iterator();
            for (i = 0; iterator.hasNext() && i < 4; i++) {
                splitResults[i] = iterator.next();
            }
            // only if we fully
            if (i == 4) {
//                    tuple = new InputTuple(1,2,3);
                tuple.setSource(splitResults[0].hashCode());
                tuple.setLabel(splitResults[1]);
                tuple.setTarget(splitResults[2].hashCode());
                tuple.setType(InputTuple.TupleType.DELETE);
                tuple.setTimestamp(lastTimetamp);
                deleteCounter++;
                return tuple;
            }
        }

        try {
            while((line = bufferedReader.readLine()) != null) {
                Iterator<String> iterator = Splitter.on('\t').trimResults().split(line).iterator();
                for(i = 0; iterator.hasNext() && i < 4; i++) {
                    splitResults[i] = iterator.next();
                }
                // only if we fully
                if(i == 4) {
//                    tuple = new InputTuple(1,2,3);
                    lastTimetamp = Long.parseLong(splitResults[3]) - startTimestamp;

                    tuple.setSource(splitResults[0].hashCode());
                    tuple.setLabel(splitResults[1]);
                    tuple.setTarget(splitResults[2].hashCode());
                    tuple.setType(InputTuple.TupleType.INSERT);
                    tuple.setTimestamp(lastTimetamp);
                    localCounter++;
                    globalCounter++;
//                    tuple = new InputTuple(Integer.parseInt(splitResults[0]), Integer.parseInt(splitResults[2]), splitResults[1]);

                    // store this tuple for later deletion
                    if(ThreadLocalRandom.current().nextInt(100) < 2 * deletionPercentage) {
                        deletionBuffer.offer(line);
                    }

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
