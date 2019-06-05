package ca.uwaterloo.cs.streamingrpq.dfa;

/**
 * Created by anilpacaci on 2019-06-05.
 */
class StatePair {
    public final int stateS, stateT;

    private StatePair(int stateS, int  stateT) {
        this.stateS = stateS;
        this.stateT = stateT;
    }

    public static StatePair createInstance(int stateS, int stateT) {
        return new StatePair(stateS,stateT);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof StatePair)) {
            return false;
        }

        StatePair sp = (StatePair) o;
        return this.stateS == sp.stateS && this.stateT == sp.stateT;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.stateS;
        result = 31 * result + this.stateT;
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder().
                append("<").append(stateS).append(",").append(stateT)
                .append(">").toString();
    }
}
