package ca.uwaterloo.cs.streamingrpq.input;

public class gMarkInputStream extends TextFileStream<Integer, Integer, String> {

    private static final int REQUIRED_FIELDS = 3;

    private static final char gMark_PREDICATE_PREFIX = 'p';

    private static final char FIELD_SEPERATOR = ' ';

    @Override
    protected int getRequiredNumberOfFields() {
        return REQUIRED_FIELDS;
    }

    @Override
    protected char getFieldSeperator() {
        return FIELD_SEPERATOR;
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
        tuple.setLabel(extractPredicate(splitResults[1]));
    }

    @Override
    protected void updateCurrentTimestamp() {
        lastTimestamp = globalCounter;
    }

    @Override
    protected void setTimestamp() {
        tuple.setTimestamp(lastTimestamp);
    }

    @Override
    public void reset() {
        close();

        open(this.filename);

        localCounter = 0;
        globalCounter = 0;
    }

    private String extractPredicate(String text) {
        return gMark_PREDICATE_PREFIX + text;
    }
}
