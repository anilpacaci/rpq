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
import java.util.concurrent.TimeUnit;

public class Yago2sHashStream extends TextFileStream {

    @Override
    protected int getRequiredNumberOfFields() {
        return 3;
    }

    @Override
    protected void setSource() {
        tuple.setSource(splitResults[0]);
    }

    @Override
    protected void setTarget() {
        tuple.setTarget(splitResults[2]);
    }

    @Override
    protected void setLabel() {
        tuple.setLabel(splitResults[1]);
    }

    @Override
    protected void updateCurrentTimestamp() {
        lastTimestamp = globalCounter;
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
