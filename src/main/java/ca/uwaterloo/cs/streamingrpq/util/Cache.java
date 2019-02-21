package ca.uwaterloo.cs.streamingrpq.util;

import ca.uwaterloo.cs.streamingrpq.core.Tuple;
import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.compound.CompoundIndex;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.iterator.IteratorUtil;

import java.util.ArrayList;
import java.util.List;

import static com.googlecode.cqengine.query.QueryFactory.*;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class Cache<S> {

    IndexedCollection<Tuple> cache;

    public Cache() {
        cache = new ConcurrentIndexedCollection<Tuple>();
        cache.addIndex(HashIndex.onAttribute(Tuple.TUPLE_TARGET));
        cache.addIndex(CompoundIndex.onAttributes(Tuple.TUPLE_SOURCE, Tuple.TUPLE_SOURCESTATE));
    }

    public boolean put(Tuple tuple) {
        return cache.add(tuple);
    }

    public boolean contains(Tuple tuple) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_SOURCE, tuple.getSource()), equal(Tuple.TUPLE_SOURCESTATE, tuple.getSourceState()), equal(Tuple.TUPLE_TARGET, tuple.getTarget()));
        return cache.retrieve(query).isNotEmpty();
    }

    public List<Tuple> retrieveByTarget(Integer target) {
        Query<Tuple> query = equal(Tuple.TUPLE_TARGET, target);
        ResultSet<Tuple> resultSet = cache.retrieve(query);
        List<Tuple> results = new ArrayList<Tuple>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<Tuple> retrieveBySourceStateAndTarget(Integer sourceState, Integer target) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_SOURCESTATE, sourceState), equal(Tuple.TUPLE_TARGET, target));
        ResultSet<Tuple> resultSet = cache.retrieve(query);
        List<Tuple> results = new ArrayList<Tuple>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<Tuple> retrieveBySource(Integer source, Integer sourceState) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_SOURCE, source), equal(Tuple.TUPLE_SOURCESTATE, sourceState));
        ResultSet<Tuple> resultSet = cache.retrieve(query);
        List<Tuple> results = new ArrayList<Tuple>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<Tuple> retrieveBySource(List<Integer> source, Integer sourceState) {
        Query<Tuple> query = and(in(Tuple.TUPLE_SOURCE, source), equal(Tuple.TUPLE_SOURCESTATE, sourceState));
        ResultSet<Tuple> resultSet = cache.retrieve(query);
        List<Tuple> results = new ArrayList<Tuple>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<Tuple> retrieveBySourceState(Integer sourceState) {
        Query<Tuple> query = equal(Tuple.TUPLE_SOURCESTATE, sourceState);
        ResultSet<Tuple> resultSet = cache.retrieve(query);
        List<Tuple> results = new ArrayList<Tuple>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }
}
