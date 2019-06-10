package ca.uwaterloo.cs.streamingrpq.util;

public enum PathSemantics {

    SIMPLE ("simple"),
    ARBITRARY ("arbitrary");

    private final String semantics;

    PathSemantics(String semantics) {
        this.semantics = semantics;
    }

    private String semantics() {
        return semantics;
    }
}
