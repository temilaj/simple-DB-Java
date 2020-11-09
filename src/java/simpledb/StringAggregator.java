package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	// we support only the count operator for strings
    private static final long serialVersionUID = 1L;
    private int gbfield;  // the index of the field to group by
    private Type gbfieldtype;  // can be any field (as we can group by any field) or null (if there is not grouping)
    
    private int afield; // index of field; doesn't actually matter here again
    private Op what; // only COUNT op here
    
    private ConcurrentHashMap<Field, Integer> count_map;
    private static final Field NO_GP = new StringField("", 2);

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	
    	if (what != Aggregator.Op.COUNT) 
    		throw new IllegalArgumentException("Illegal Operation for strings; Only count supported");
    	this.what = what;
    	
    	count_map = new ConcurrentHashMap<Field, Integer>();	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field key; // the group by key
    	if (gbfield == Aggregator.NO_GROUPING) 
    	{
    		key = NO_GP;
    	}
    	else
    	{
    		key=tup.getField(this.gbfield);
    	}
    	
		if (!(count_map.containsKey(key))) 
		{
			// for the first insertion - 0 or 1 ?
			count_map.put(key, 1);
						
		} 
		else 
		{
    		count_map.put(key,count_map.get(key) + 1);
		}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	TupleDesc t_desc;
    	Tuple t;
    	List<Tuple> tuple_list = new ArrayList<Tuple>();
    	if (this.gbfield == Aggregator.NO_GROUPING) 
    	{
    		t_desc = new TupleDesc(new Type[] { Type.INT_TYPE });
    		t = new Tuple(t_desc);
    		int val = count_map.get(NO_GP);
    		t.setField(0, new IntField(val));
    		tuple_list.add(t);
    	}
    	else 
    	{
    		t_desc = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
	    	Enumeration<Field> keys = count_map.keys();    	
	    	while (keys.hasMoreElements()) 
	    	{
	    		t = new Tuple(t_desc);
	    		Field key = keys.nextElement();
	    		int val = count_map.get(key);
	    		t.setField(0, key);
	    		t.setField(1, new IntField(val));
	    		tuple_list.add(t);
	    	}
    	}
    	return new TupleIterator(t_desc, tuple_list);
    }

}
