package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    // Array to hold all tuple descriptiors
    private ArrayList<TDItem> arr = new ArrayList<>();
//    private TDItem[] arr;	
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	return arr.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	// insert into the TDItem arr
    	for(int i=0;i<typeAr.length;++i)
    	{
    		TDItem temp =  new TDItem(typeAr[i],fieldAr[i]);
    		arr.add(temp);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	for(int i=0;i<typeAr.length;++i)
    	{
    		String s = new String();
    		TDItem temp =  new TDItem(typeAr[i],s);
    		arr.add(temp);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return arr.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
//        return null;
    	String s = new String();
    	try {
    		s = arr.get(i).fieldName;
    	}
    	catch(Exception e)
    	{
    		throw new NoSuchElementException();
    	}
    	return s;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	try {
    		return arr.get(i).fieldType;
    	}
    	catch(Exception e)
    	{
    		throw new NoSuchElementException();
    	}
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here

    		for(int i=0;i<arr.size();++i)
    		{
    			if(arr.get(i).fieldName.equals(name))
    				{
    				return i;
    				}
    		}

    		throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int summ = 0;
    	for(int i=0;i<arr.size();++i)
    	{
    		summ = summ + arr.get(i).fieldType.getLen();
    	}
    	return summ;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	int size_target = td1.numFields() + td2.numFields();
    	Type[] type_arr = new Type[size_target];
    	String[] string_arr= new String[size_target];
    	int i=0;
    	for(int j=0;j<td1.numFields();++j)
    	{
    		type_arr[i]=td1.getFieldType(j);
    		string_arr[i++] = td1.getFieldName(j);
    	}
    	
    	for(int j=0;j<td2.numFields();++j)
    	{
    		type_arr[i]=td2.getFieldType(j);
    		string_arr[i++] = td2.getFieldName(j);
    	}
    	
    	TupleDesc td_final =  new TupleDesc(type_arr,string_arr);
    	
    	return td_final;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	// number and type of arguments should be the same
    	if(o==null)
    		return false;
    	
    	if(!(o instanceof TupleDesc))
    		return false;
    	
    	TupleDesc checking = (TupleDesc) o;
    	
    	if(checking.numFields()!=this.numFields())
    		return false;
    	
    	for(int i=0;i<checking.numFields();++i)
    	{
    		if(checking.getFieldType(i)!=this.getFieldType(i))
    			{
    				return false;
    			}
    	}
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
    	return this.toString().hashCode(); // use library function for simplicity
        
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String ans = new String();
    	for(int i=0;i<arr.size();++i)
    	{
    		ans = ans + arr.get(i).toString();
    	}
        return ans;
    }
}
