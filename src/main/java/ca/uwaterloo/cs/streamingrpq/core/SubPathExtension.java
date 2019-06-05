package ca.uwaterloo.cs.streamingrpq.core;

import ca.uwaterloo.cs.streamingrpq.data.RAPQTuple;

/**
 * Created by anilpacaci on 2019-03-02.
 */
public class SubPathExtension {
    private RAPQTuple tuple;

    private Integer originatingState;

    public SubPathExtension(RAPQTuple tuple, Integer originatingState) {
        this.tuple = tuple;
        this.originatingState = originatingState;
    }

    public RAPQTuple getTuple() {
        return tuple;
    }

    public Integer getOriginatingState() {
        return originatingState;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(tuple.toString()).append(", originatingState: ").append(originatingState).toString();
    }
}
