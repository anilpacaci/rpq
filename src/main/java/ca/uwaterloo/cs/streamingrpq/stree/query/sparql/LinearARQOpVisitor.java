package ca.uwaterloo.cs.streamingrpq.stree.query.sparql;

import ca.uwaterloo.cs.streamingrpq.stree.query.NFA;
import ca.uwaterloo.cs.streamingrpq.stree.query.NFAAutomataBuilder;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.path.Path;

import java.util.Stack;

public class LinearARQOpVisitor extends OpVisitorBase {

    Stack<NFA<String>> nfaStack;
    private NFAAutomataBuilder<String> automataBuilder;

    public LinearARQOpVisitor() {
        nfaStack = new Stack<>();
        automataBuilder = new NFAAutomataBuilder<>();
    }

    public NFA<String> getAutomaton() {
        return nfaStack.pop();
    }

    @Override
    public void visit(OpGroup opGroup) {
        return;
    }

    @Override
    public void visit(OpPath opPath) {
        ARQPathVisitor visitor = new ARQPathVisitor();
        Path path = opPath.getTriplePath().getPath();
        path.visit(visitor);

        NFA<String> nfa = visitor.getAutomaton();

        nfaStack.push(nfa);

        return;
    }

    @Override
    public void visit(OpSequence opSequence)  {
        // this is a linear chain processor so assumption is that all conjuncts form a linear chain.
        // thus we can simply concataneta all conjuncts
        while(nfaStack.size() > 1) {
            NFA<String> rightNFA = nfaStack.pop();
            NFA<String> leftNFA = nfaStack.pop();

            NFA<String> resultNFA = automataBuilder.concenetation(leftNFA, rightNFA);
            nfaStack.push(resultNFA);
        }

        return;
    }
}
