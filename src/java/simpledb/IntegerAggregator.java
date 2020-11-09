package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;  // the index of the field to group by
    private Type gbfieldtype;  // can be any field (as we can group by any field) or null (if there is not grouping)
    private int afield; // this has to be an INT field; index of field
    private Op what; // operation to perform - eg SUM, COUNT, AVG, etc
    private ConcurrentHashMap<Field, Integer> value_map;  // for all ops except count
    private ConcurrentHashMap<Field, Integer> count_map;  // for count op especially
    // we need two maps because of the avg operator where we have both values and  counts
    private static final Field NO_GP = new IntField(-1); // a key to represent no grouping

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	value_map = new ConcurrentHashMap<Field, Integer>();
    	count_map = new ConcurrentHashMap<Field, Integer>();
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
    	int val = ((IntField) (tup.getField(this.afield))).getValue(); // value of the aggregate field

		if (!(this.value_map.containsKey(key))) 
		{
			// for the first insertion
			value_map.put(key, val);
			count_map.put(key, 1);
			
		} 
		else 
		{
    		value_map.put(key,this.perform_op(value_map.get(key), val));
    		count_map.put(key,count_map.get(key) + 1);
		}
    	
    }
    
    
    private int perform_op(int x, int y) {
    	
    	if (this.what == Aggregator.Op.AVG || this.what == Aggregator.Op.COUNT || this.what == Aggregator.Op.SUM )
		{
    		return x + y;
		}
    	else if(this.what == Aggregator.Op.MIN)
    		return Math.min(x, y);
    	else if(this.what == Aggregator.Op.MAX)
    		return Math.max(x, y);
    	
    	return 0;
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
    		int val = value_map.get(NO_GP);
    		if (this.what == Aggregator.Op.AVG)
    		{
    			val = val/count_map.get(NO_GP);
    		}
    		else if (this.what == Aggregator.Op.COUNT)
    		{
    			val = count_map.get(NO_GP);
    		}
    		t.setField(0, new IntField(val));
    		tuple_list.add(t);
    	}
    	else 
    	{
    		t_desc = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
	    	Enumeration<Field> keys = value_map.keys();    	
	    	while (keys.hasMoreElements()) 
	    	{
	    		t = new Tuple(t_desc);
	    		Field key = keys.nextElement();
	    		int val = value_map.get(key);
	    		if (this.what == Aggregator.Op.AVG)
	    		{
	    			val = val/this.count_map.get(key);
	    		}
	    		else if (this.what == Aggregator.Op.COUNT) 
	    		{
	    			val = count_map.get(key);
	    		}
	    		t.setField(0, key);
	    		t.setField(1, new IntField(val));
	    		tuple_list.add(t);
	    	}
    	}
    	return new TupleIterator(t_desc, tuple_list);
    	
    }

}
