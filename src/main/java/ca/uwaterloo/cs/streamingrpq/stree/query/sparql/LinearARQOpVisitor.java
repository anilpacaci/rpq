package ca.uwaterloo.cs.streamingrpq.stree.query.sparql;

import ca.uwaterloo.cs.streamingrpq.stree.query.NFA;
import ca.uwaterloo.cs.streamingrpq.stree.query.NFABuilder;
import com.google.common.collect.Maps;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.path.Path;

import java.util.List;
import java.util.Map;
import java.util.Stack;

public class LinearARQOpVisitor extends OpVisitorBase {

    Stack<NFA<String>> nfaStack;
    private NFABuilder<String> nfaBuilder;

    public LinearARQOpVisitor() {
        nfaStack = new Stack<>();
        nfaBuilder = new NFABuilder<>();
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

            NFA<String> resultNFA = nfaBuilder.concenetation(leftNFA, rightNFA);
            nfaStack.push(resultNFA);
        }

        return;
    }
}
