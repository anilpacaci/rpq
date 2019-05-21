package ca.uwaterloo.cs.streamingrpq.data;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.compound.CompoundIndex;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.common.NoSuchObjectException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.googlecode.cqengine.query.QueryFactory.*;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class Delta {

    IndexedCollection<Tuple> cache;

    public Delta() {
        cache = new ConcurrentIndexedCollection<Tuple>();
        cache.addIndex(HashIndex.onAttribute(Tuple.TUPLE_TARGET));
        cache.addIndex(CompoundIndex.onAttributes(Tuple.TUPLE_TARGET, Tuple.TUPLE_TARGETSTATE));
    }


    public boolean contains(Tuple tuple) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_SOURCE, tuple.getSource()), equal(Tuple.TUPLE_TARGETSTATE, tuple.getTargetState()), equal(Tuple.TUPLE_TARGET, tuple.getTarget()));
        return cache.retrieve(query).isNotEmpty();
    }

    public List<Tuple> retrieveByTarget(Integer target) {
        Query<Tuple> query = equal(Tuple.TUPLE_TARGET, target);
        ResultSet<Tuple> resultSet = cache.retrieve(query);
        List<Tuple> results = new ArrayList<Tuple>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<Tuple> retrieveByTargetAndTargetState(Integer target, Integer targetState) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_TARGETSTATE, targetState), equal(Tuple.TUPLE_TARGET, target));
        ResultSet<Tuple> resultSet = cache.retrieve(query);
        List<Tuple> results = new ArrayList<Tuple>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<Tuple> retrieveAll() {
        List<Tuple> results = cache.stream().collect(Collectors.toList());
        return results;
    }

    public void removeAll() {
        cache.clear();
    }

    public void add(Tuple candidateTuple) {
        cache.add(candidateTuple);
    }

    public void addPredecessor(Tuple tuple, ProductNode predecessor) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_SOURCE, tuple.getSource()), equal(Tuple.TUPLE_TARGETSTATE, tuple.getTargetState()), equal(Tuple.TUPLE_TARGET, tuple.getTarget()));
        cache.retrieve(query).uniqueResult().addPredecessor(predecessor);
    }
}
