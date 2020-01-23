package ca.uwaterloo.cs.streamingrpq.input;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class LDBCStream extends TextFileStream<Integer, Integer, String> {

    private final Logger logger = LoggerFactory.getLogger(LDBCStream.class);

    private static int FIELD_COUNT = 4;

    @Override
    protected int getRequiredNumberOfFields() {
        return FIELD_COUNT;
    }

    @Override
    protected void setSource() {
        tuple.setSource(splitResults[0].hashCode());
    }

    @Override
    protected void setTarget() {
        tuple.setTarget(splitResults[2].hashCode());
    }

    @Override
    protected void setLabel() {
        tuple.setLabel(splitResults[1]);
    }

    @Override
    protected void updateCurrentTimestamp() {
        lastTimestamp = Long.parseLong(splitResults[3]) - startTimestamp;
    }

    @Override
    protected void setTimestamp() {
        tuple.setTimestamp(lastTimestamp);
    }

    public void reset() {
        close();

        open(this.filename);

        localCounter = 0;
        globalCounter = 0;
    }
}
