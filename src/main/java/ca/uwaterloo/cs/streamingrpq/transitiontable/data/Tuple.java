package ca.uwaterloo.cs.streamingrpq.transitiontable.data;

public interface Tuple {
    int getSource();

    int getTarget();

    int getTargetState();

    ProductNode getTargetNode();
}
