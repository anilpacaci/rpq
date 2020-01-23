package ca.uwaterloo.cs.streamingrpq.input;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.Iterator;

public class StackOverflowStream extends TextFileStream {

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
