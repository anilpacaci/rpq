package ca.uwaterloo.cs.streamingrpq.input;

import com.google.common.base.Splitter;

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

public class SimpleTextStreamWithExplicitDeletions extends TextFileStream<Integer, Integer, String> {


    public InputTuple<Integer, Integer, String> next() {
        String line = null;
        InputTuple tuple = null;
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

                    if(splitResults[3].equals("+")) {
                        tuple.setType(InputTuple.TupleType.INSERT);
                    } else {
                        tuple.setType(InputTuple.TupleType.DELETE);
                    }

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

    @Override
    protected int parseLine(String line) {
        int i = 0;
        Iterator<String> iterator = Splitter.on('\t').trimResults().split(line).iterator();
        for(i = 0; iterator.hasNext() && i < 4; i++) {
            splitResults[i] = iterator.next();
        }

        return i;
    }

    @Override
    protected int getRequiredNumberOfFields() {
        return 4;
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
