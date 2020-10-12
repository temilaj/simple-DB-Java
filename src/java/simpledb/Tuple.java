package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ArrayList;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private TupleDesc schema;  // store the tuple schema
    private RecordId rec_id; // store the record id for that tuple
    private ArrayList<Field> tuple_fields; // store the actual fields

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
    	
    	if(td.numFields()==0)
    		return;
    	
    	schema = td;
    	tuple_fields = new ArrayList<Field>();
    	for(int i=0;i<schema.numFields();++i)
    	{
    		Field f=null;
    		tuple_fields.add(f);
    	}	
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
    	return rec_id;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	rec_id = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
    	if(i>=0 && i< tuple_fields.size())  // sanity check
    		tuple_fields.set(i,f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i)  throws NoSuchElementException  {
        // some code goes here
        try {
        	return tuple_fields.get(i);
        }
    	catch(Exception e)
    	{
    		throw new NoSuchElementException();
    	}
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
//        throw new UnsupportedOperationException("Implement this");
    	String ans = new String();
    	
    	for(int i=0;i<tuple_fields.size();++i)
    	{
    		ans = ans + tuple_fields.get(i).toString();
    		ans = ans + "\t";
    	}
    	return ans;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
//        return null;
    	return tuple_fields.iterator();
    }

    /**
     * reset the TupleDesc of the tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here

    	if(td.numFields()==0)
    		return;
    	
    	schema = td;
    	
    	for(int i=0;i<schema.numFields();++i)
    	{
    		Field f=null;
    		tuple_fields.add(f);
    	}
    }
}
