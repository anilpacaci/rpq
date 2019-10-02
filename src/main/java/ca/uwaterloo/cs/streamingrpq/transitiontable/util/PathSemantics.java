package ca.uwaterloo.cs.streamingrpq.transitiontable.util;


import java.util.HashMap;
import java.util.Map;

public enum PathSemantics {

    SIMPLE ("simple"),
    ARBITRARY ("arbitrary");

    private final String semantics;

    private static final Map<String, PathSemantics> BY_LABEL = new HashMap<>();

    static {
        for(PathSemantics p : values()) {
            BY_LABEL.put(p.semantics, p);
        }
    }

    PathSemantics(String semantics) {
        this.semantics = semantics;
    }

    @Override
    public String toString() {
        return semantics;
    }

    public static PathSemantics fromValue(String semantics) {
        return BY_LABEL.get(semantics);
    }
}
