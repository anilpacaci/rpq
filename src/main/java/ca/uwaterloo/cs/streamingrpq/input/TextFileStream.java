package ca.uwaterloo.cs.streamingrpq.input;

import ca.uwaterloo.cs.streamingrpq.stree.engine.WindowedRPQ;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.soap.Text;
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

/**
 * Created by anilpacaci on 2019-01-31.
 */
public abstract class TextFileStream<S, T, L> {

    private final Logger logger = LoggerFactory.getLogger(TextFileStream.class);

    protected FileReader fileStream;
    protected BufferedReader bufferedReader;

    protected String filename;

    protected ScheduledExecutorService executor;

    protected int localCounter = 0;
    protected int globalCounter = 0;
    protected int deleteCounter = 0;

    protected long startTimestamp = -1L;
    protected long lastTimestamp = Long.MIN_VALUE;

    protected Queue<String> deletionBuffer;
    protected int deletionPercentage = 0;

    protected String splitResults[];

    protected InputTuple<S, T, L> tuple = null;

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
                deleteCounter++;
            }
        };

        this.executor = Executors.newScheduledThreadPool(1);
        this.executor.scheduleAtFixedRate(counterRunnable, 1, 1, TimeUnit.SECONDS);

        this.splitResults = new String[4];
        this.deletionBuffer = new ArrayDeque<String>();
        this.tuple = new InputTuple(null, null, null, 0);
    }

    public void open(String filename, int maxSize) {
        this.startTimestamp = 0L;
        open(filename);
    }

    public void open(String filename, int maxSize, long startTimestamp, int deletionPercentage) {
        this.startTimestamp = startTimestamp;
        open(filename);
    }

    /**
     * Generate the next input tuple
     * @return
     */
    public InputTuple<S, T, L> next() {
        String line = null;

        //generate negative tuple if necessary
        if(!deletionBuffer.isEmpty() && ThreadLocalRandom.current().nextInt(100) < deletionPercentage) {
            line = deletionBuffer.poll();
            int i = parseLine(line);
            // only if we fully
            if (i == getRequiredNumberOfFields()) {
                setSource();
                setLabel();
                setTarget();
                setTimestamp();

                tuple.setType(InputTuple.TupleType.DELETE);

                deleteCounter++;
                return tuple;
            }
        }

        try {
            while((line = bufferedReader.readLine()) != null) {
                int i = parseLine(line);
                // only if we fully
                if(i == 4) {
                    setSource();
                    setLabel();
                    setTarget();
                    updateCurrentTimestamp();
                    setTimestamp();

                    tuple.setType(InputTuple.TupleType.INSERT);

                    localCounter++;
                    globalCounter++;

                    // store this tuple for later deletion
                    if(ThreadLocalRandom.current().nextInt(100) < 2 * deletionPercentage) {
                        deletionBuffer.offer(line);
                    }

                    break;
                }
            }
        } catch (IOException e) {
            logger.error("Parsing input line: {}", line, e);
            return null;
        }

        if (line == null) {
            return null;
        }

        return tuple;
    }

    /**
     * the total number of fields that needs to be parsed from a line
     * @return
     */
    protected abstract int getRequiredNumberOfFields();

    /**
     * Parse a single line from the source and populate splitResults for the creation of the next tuple
     * @param line
     * @return total number of fields parsed from the line
     */
    protected abstract int parseLine(String line);

    protected abstract void setSource();

    protected abstract void setTarget();

    protected abstract void setLabel();

    protected abstract void updateCurrentTimestamp();

    protected abstract void setTimestamp();

    public void close() {
        try {
            bufferedReader.close();
            fileStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        executor.shutdown();
    }

    public abstract void reset();

}
