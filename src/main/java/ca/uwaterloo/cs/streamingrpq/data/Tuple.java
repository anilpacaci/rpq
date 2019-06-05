package ca.uwaterloo.cs.streamingrpq.data;

public interface Tuple {
    int getSource();

    int getTarget();

    int getTargetState();

    ProductNode getTargetNode();
}
