package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File f;

    private final TupleDesc td;

    private static final class HeapFileIterator implements DbFileIterator {
        private final TransactionId tid;
        private final HeapFile heapFile;

        private Iterator<Tuple> tupleIterator;
        private int index;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.tid = tid;
            this.heapFile = file;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            index = 0;
            tupleIterator = getTupleIterator(index);
        }

        private Iterator<Tuple> getTupleIterator(int pageNumber) throws TransactionAbortedException, DbException {
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            } else {
                throw new DbException("Page number out of range");
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) {
                return false;
            }
            while(!tupleIterator.hasNext()){
                index++;
                if(index < heapFile.numPages()){
                    tupleIterator = getTupleIterator(index);
                }else{
                    return false;
                }
            }
            return true;

        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator == null || !tupleIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
        int tableId = pid.getTableId();
        int pageNumber = pid.getPageNumber();
        int offset = pageNumber * BufferPool.getPageSize();
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(f, "r");
            //In the buffer pool, the page size should greater than 4096
            if (randomAccessFile.length() < (long) (pageNumber + 1) * BufferPool.getPageSize()) {
                throw new IllegalArgumentException();
            }
            byte[] data = new byte[BufferPool.getPageSize()];
            // move the file pointer to the start of the page
            randomAccessFile.seek(offset);
            randomAccessFile.read(data, 0, BufferPool.getPageSize());
            HeapPageId id = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(id, data);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
                try {
                    if(randomAccessFile != null){
                    randomAccessFile.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
            throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        PageId pageId = page.getId();
        int pageNo = pageId.getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        byte[] pageData = page.getPageData();

        RandomAccessFile file = new RandomAccessFile(this.f, "rw");
        file.seek(offset);
        file.write(pageData);
        file.close();
        page.markDirty(false,null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // calculate the number of pages in the file
        return (int) Math.floor(getFile().length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        ArrayList<Page> pageList = new ArrayList<Page>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() == 0)
                continue;
            page.insertTuple(t);
            pageList.add(page);
            return pageList;
        }
        // no empty slots, add a new page
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
        byte[] data = HeapPage.createEmptyPageData();
        bw.write(data);
        bw.close();
        HeapPageId pid = new HeapPageId(getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        pageList.add(page);
        return pageList;

}

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
       HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFileIterator(this, tid);
    }

}

