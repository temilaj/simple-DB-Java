package simpledb;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private ConcurrentHashMap<PageId,Page> page_hash;
    // ok so this stores the Pagid and page
    int num_pages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	page_hash = new ConcurrentHashMap<PageId,Page>();
    	num_pages=numPages;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	// simple as of now
    	// if page in buffer return directly
    	// if space still left read from memory and add
    	// if no space throw db exception
    	if (page_hash.containsKey(pid))
        {
            return page_hash.get(pid);

        }
    	if (page_hash.size() == num_pages)
        {
            evictPage();
        }
    	if (page_hash.size() < num_pages)
        {
            // read from memory
            int tableid = pid.getTableId();
            DbFile f = Database.getCatalog().getDatabaseFile(tableid);
            // now read from page from file
            Page p = f.readPage(pid);
            page_hash.put(pid, p);
            return p;
        }
        else
        {
            throw new DbException("Buffer pool is full");
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
//        HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
//
//        // get all pages that have been updated.
//        ArrayList<Page> updatedPages = file.insertTuple(tid, t);

        ArrayList<Page> updatedPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        // TODO implement Lock acquisition. (not needed for lab2)
        for (Page page : updatedPages)
        {
            PageId pageId = page.getId();
            page.markDirty(true, tid);
            page_hash.put(pageId, page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        try
        {
        	ArrayList<Page> updatedPages = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
            for (Page page : updatedPages)
            {
                PageId pageId = page.getId();
                page.markDirty(true, tid);
                // replace old page with updated page
                page_hash.put(pageId, page);
            }
        }
        catch (Exception e)
        {
        	e.printStackTrace();
            throw new IOException("Error deleting tuple from table " + tableId);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pageId : page_hash.keySet())
        {
            flushPage(pageId);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        page_hash.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        if (page_hash.containsKey(pid))
        {
            Page page = this.page_hash.get(pid);
            if (page.isDirty() != null)
            {
              // write page to disk.
            	Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
                // mark page as not dirty
                page.markDirty(false, null);
            }
        }
        else
        {
            throw new IOException();
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        for (PageId pageId : page_hash.keySet())
        {
            try
            {
                // flush page to disk
                flushPage(pageId);
                // Discard page from buffer pool.
                page_hash.remove(pageId);
            }
            catch (Exception e)
            {
                throw new DbException("Error flushing page " + pageId + " during eviction: " + e.getMessage());
            }
        }
    }

}
