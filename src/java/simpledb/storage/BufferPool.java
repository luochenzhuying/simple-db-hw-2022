package simpledb.storage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;

    private LRUCache lruCache;

    private LockManager lockManager;

    public enum LockType {
        SHARE_LOCK, EXCLUSIVE_LOCK
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // TODO: some code goes here
        this.numPages = numPages;
        lruCache = new LRUCache(numPages);
        lockManager = new LockManager();
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
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        LockType lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = LockType.SHARE_LOCK;
        } else {
            lockType = LockType.EXCLUSIVE_LOCK;
        }
        try {
            // if fail to acquire lock, abort the transaction
            if (!lockManager.acquireLock(pid, tid, lockType, 0)) {
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (lruCache.get(pid) == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            lruCache.put(pid, page);
        }
        return lruCache.get(pid);
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }


    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            rollBack(tid);
        }
        lockManager.releasePagesByTid(tid);
    }

    @SneakyThrows
    private synchronized void rollBack(TransactionId tid) {
        for (Map.Entry<PageId, LRUCache.Node> group : lruCache.getEntrySet()) {
            PageId pageId = group.getKey();
            Page page = group.getValue().value;
            if (tid.equals(page.isDirty())) {
                int tableId = pageId.getTableId();
                DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
                Page readPage = dbFile.readPage(pageId);
                lruCache.removeByKey(group.getKey());
                lruCache.put(pageId, readPage);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(f.insertTuple(tid, t), tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> updatePages = f.deleteTuple(tid, t);
        updateBufferPool(updatePages, tid);
    }

    private void updateBufferPool(List<Page> updatePages, TransactionId tid) throws DbException {
        for (Page page : updatePages) {
            page.markDirty(true, tid);
            lruCache.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, LRUCache.Node> group : lruCache.getEntrySet()) {
            Page page = group.getValue().value;
            if (page.isDirty() != null) {
                flushPage(group.getKey());
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
        lruCache.removeByKey(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        Page page = lruCache.get(pid);
        if (page == null) {
            return;
        }
        TransactionId tid = page.isDirty();
        if (tid != null) {
            Page beforeImage = page.getBeforeImage();
            Database.getLogFile().logWrite(tid, beforeImage, page);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, LRUCache.Node> group : lruCache.getEntrySet()) {
            PageId pid = group.getKey();
            Page flushPage = group.getValue().value;
            TransactionId pageTid = flushPage.isDirty();
            Page before = flushPage.getBeforeImage();
            flushPage.setBeforeImage();
            if (pageTid != null && pageTid.equals(tid)) {
                Database.getLogFile().logWrite(tid, before, flushPage);
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(flushPage);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    private static class LRUCache {
        int capacity;
        int size;
        ConcurrentHashMap<PageId, Node> map;
        Node head = new Node(null, null);
        Node tail = new Node(null, null);

        public LRUCache(int capacity) {
            this.capacity = capacity;
            this.size = 0;
            this.map = new ConcurrentHashMap<>();
            head.next = tail;
            tail.prev = head;
        }

        public synchronized Page get(PageId key) {
            if (contain(key)) {
                remove(map.get(key));
                moveToHead(map.get(key));
                return map.get(key).value;
            } else {
                return null;
            }
        }

        public synchronized void put(PageId key, Page value) throws DbException {
            Node newNode = new Node(key, value);
            if (map.containsKey(key)) {
                remove(map.get(key));
            } else {
                size++;
                if (size > capacity) {
                    Node removeNode = tail.prev;
                    //discard not dirty page
                    while (removeNode.value.isDirty() != null && removeNode != head) {
                        removeNode = removeNode.prev;
                        if (removeNode == head || removeNode == tail) {
                            throw new DbException("no page can be evicted");
                        }
                    }
                    map.remove(removeNode.key);
                    remove(removeNode);
                    size--;
                }
            }
            moveToHead(newNode);
            map.put(key, newNode);
        }

        public synchronized void remove(Node node) {
            Node prev = node.prev;
            Node next = node.next;
            prev.next = next;
            next.prev = prev;
        }

        public synchronized void moveToHead(Node node) {
            Node next = head.next;
            head.next = node;
            node.prev = head;
            node.next = next;
            next.prev = node;
        }

        public synchronized int getSize() {
            return size;
        }

        public synchronized void removeByKey(PageId key) {
            Node node = map.get(key);
            remove(node);
        }

        public synchronized boolean contain(PageId key) {
            for (PageId pageId : map.keySet()) {
                if (pageId.equals(key)) {
                    return true;
                }
            }
            return false;
        }

        private static class Node {
            PageId key;
            Page value;
            Node prev;
            Node next;

            public Node(PageId key, Page value) {
                this.key = key;
                this.value = value;
            }
        }

        public Set<Map.Entry<PageId, Node>> getEntrySet() {
            return map.entrySet();
        }
    }

    @AllArgsConstructor
    @Data
    private static class PageLock {
        private TransactionId tid;
        private PageId pid;
        private LockType type;
    }

    private static class LockManager {
        @Getter
        public ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap;

        public LockManager() {
            lockMap = new ConcurrentHashMap<>();
        }

        /**
         * Return true if the specified transaction has a lock on the specified page
         */
        public boolean holdsLock(TransactionId tid, PageId p) {
            // TODO: some code goes here
            // not necessary for lab1|lab2
            if (lockMap.get(p) == null) {
                return false;
            }
            return lockMap.get(p).get(tid) != null;
        }

        public synchronized boolean acquireLock(PageId pageId, TransactionId tid, LockType requestLock, int reTry) throws TransactionAbortedException, InterruptedException {
            // 3 times reTry
            if (reTry == 3) {
                return false;
            }
            // if the page is not in the lockMap, add it
            if (lockMap.get(pageId) == null) {
                return putLock(tid, pageId, requestLock);
            }
            // if the page is in the lockMap, check
            ConcurrentHashMap<TransactionId, PageLock> tidLocksMap = lockMap.get(pageId);
            // the tid does not have a lock on the page
            if (tidLocksMap.get(tid) == null) {
                // if requestLock is X lock
                if (requestLock == LockType.EXCLUSIVE_LOCK) {
                    wait(100);
                    return acquireLock(pageId, tid, requestLock, reTry + 1);
                } else if (requestLock == LockType.SHARE_LOCK) {
                    // if the lock size on the page greater than 0, the locks are all S lock,because x lock acquired by one transaction
                    if (tidLocksMap.size() > 1) {
                        // all the locks are S lock, the tid can acquire S lock
                        return putLock(tid, pageId, requestLock);
                    } else {
                        Collection<PageLock> values = tidLocksMap.values();
                        for (PageLock value : values) {
                            if (value.getType() == LockType.EXCLUSIVE_LOCK) {
                                wait(100);
                                return acquireLock(pageId, tid, requestLock, reTry + 1);
                            } else {
                                return putLock(tid, pageId, requestLock);
                            }
                        }
                    }
                }
            } else {
                // the tid has a lock on the page
                if (requestLock == LockType.SHARE_LOCK) {
                    tidLocksMap.remove(tid);
                    return putLock(tid, pageId, requestLock);
                } else {
                    // if self's lock is exclusive lock, return true
                    if (tidLocksMap.get(tid).getType() == LockType.EXCLUSIVE_LOCK) {
                        return true;
                    } else {
                        // if self's lock is share lock, determine whether the page has other locks
                        if (tidLocksMap.size() > 1) {
                            wait(100);
                            return acquireLock(pageId, tid, requestLock, reTry + 1);
                        } else {
                            // if the page has only one lock, and the lock is self's lock, upgrade the lock
                            tidLocksMap.remove(tid);
                            return putLock(tid, pageId, requestLock);
                        }
                    }
                }
            }
            return false;
        }

        public boolean putLock(TransactionId tid, PageId pageId, LockType requestLock) {
            ConcurrentHashMap<TransactionId, PageLock> tidLocksMap = lockMap.get(pageId);
            // if the page does not have a lock,
            if (tidLocksMap == null) {
                tidLocksMap = new ConcurrentHashMap<>();
                lockMap.put(pageId, tidLocksMap);
            }
            PageLock pageLock = new PageLock(tid, pageId, requestLock);
            tidLocksMap.put(tid, pageLock);
            lockMap.put(pageId, tidLocksMap);
            return true;
        }

        public void releasePagesByTid(TransactionId tid) {
            Set<PageId> pageIds = lockMap.keySet();
            for (PageId pageId : pageIds) {
                releasePage(tid, pageId);
            }
        }


        public synchronized void releasePage(TransactionId tid, PageId pid) {
            if (holdsLock(tid, pid)) {
                ConcurrentHashMap<TransactionId, PageLock> tidLocks = lockMap.get(pid);
                tidLocks.remove(tid);
                if (tidLocks.isEmpty()) {
                    lockMap.remove(pid);
                }
                this.notifyAll();
            }
        }
    }

}

