package ca.uwaterloo.cs.streamingrpq.input;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public interface TextStream<S, T, L> {

    boolean isOpen();

    void open(String filename);

    void open(String filename, int size);

    void open(String filename, int size, long startTimestamp, int deletionPercentage);

    InputTuple<S, T, L> next();

    void close();

    void reset();

}
