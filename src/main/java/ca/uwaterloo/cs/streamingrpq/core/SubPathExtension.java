package ca.uwaterloo.cs.streamingrpq.core;

import com.googlecode.cqengine.query.simple.In;

/**
 * Created by anilpacaci on 2019-03-02.
 */
public class SubPathExtension {
    private SubPath subPath;

    private Integer originatingState;

    public SubPathExtension(SubPath subPath, Integer originatingState) {
        this.subPath = subPath;
        this.originatingState = originatingState;
    }

    public SubPath getSubPath() {
        return subPath;
    }

    public Integer getOriginatingState() {
        return originatingState;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(subPath.toString()).append(", originatingState: ").append(originatingState).toString();
    }
}
