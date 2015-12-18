package milleniumbug.sbd_02_b_drzewo;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import milleniumbug.sbd_02_b_drzewo.BNode.BNodeElement;
import milleniumbug.sbd_02_b_drzewo.generic_caches.Cache;
import milleniumbug.sbd_02_b_drzewo.generic_caches.MapSynchronizer;
import milleniumbug.sbd_02_b_drzewo.generic_caches.NoopCache;

public class BTree implements AutoCloseable {

    private long free_list_nodes = BNode.not_a_child;
    private long next_allocate_nodes;
    private long free_list_data;
    private long next_allocate_data;
    private long root = 1;
    Cache<Long, BNode> access;

    public BTree(File btree, File data) {
        //access = new BNodeCache(btree);
    }

    // temporary
    private BTree() {
        next_allocate_data = 42;
        next_allocate_nodes = 42;
    }

    private long getNextAllocatedNodeAddress() {
        long old = next_allocate_nodes;
        next_allocate_nodes += 1;
        return old;
    }

    private long allocateNode() {
        if (free_list_nodes == BNode.not_a_child) {
            return getNextAllocatedNodeAddress();
        }
        BNode.BNodeElement el = access.lookup(free_list_nodes).getSentinel();
        assert el.getLeftChild() == BNode.not_a_child;
        assert el.getKey() == BNode.not_a_key;
        long allocated = free_list_nodes;
        free_list_nodes = el.getValuePosition();
        return allocated;
    }

    private void deallocateNode(long x) {
        access.replace(x, BNode.freeNode(free_list_nodes, x));
        free_list_nodes = x;
    }

    static private BTree testDataBasic() {
        BTree t = new BTree();
        t.free_list_data = 0;
        t.free_list_nodes = 0;
        t.access = new NoopCache<>(new MapSynchronizer<>(new HashMap<Long, BNode>() {
            {
                put(1L, new BNode(Arrays.asList(
                        new BNode.BNodeElement(5L, 0L, 2L),
                        new BNode.BNodeElement(20L, 0L, 3L),
                        new BNode.BNodeElement(BNode.not_a_key, 0L, 4L)), 1L));
                put(2L, new BNode(Arrays.asList(
                        new BNode.BNodeElement(1L, 0L, BNode.not_a_child),
                        new BNode.BNodeElement(2L, 0L, BNode.not_a_child),
                        new BNode.BNodeElement(BNode.not_a_key, 0L, BNode.not_a_child)), 2L));
                put(3L, new BNode(Arrays.asList(
                        new BNode.BNodeElement(10L, 0L, BNode.not_a_child),
                        new BNode.BNodeElement(15L, 0L, BNode.not_a_child),
                        new BNode.BNodeElement(BNode.not_a_key, 0L, BNode.not_a_child)), 3L));
                put(4L, new BNode(Arrays.asList(
                        new BNode.BNodeElement(30L, 0L, BNode.not_a_child),
                        new BNode.BNodeElement(40L, 0L, BNode.not_a_child),
                        new BNode.BNodeElement(BNode.not_a_key, 0L, BNode.not_a_child)), 4L));
            }
        }));
        return t;
    }

    static private BTree testDataEmpty() {
        BTree t = new BTree();
        t.free_list_data = 0;
        t.free_list_nodes = 0;
        t.access = new NoopCache<>(new MapSynchronizer<>(new HashMap<Long, BNode>() {
            {
                put(1L, new BNode(Arrays.asList(
                        new BNode.BNodeElement(BNode.not_a_key, 0L, BNode.not_a_child)), 1L));
            }
        }));
        return t;
    }

    private BPath resolve(final long key) {
        BNode root_node = access.lookup(root);
        BPath res = new BPath();

        BNode current_node = root_node;
        List<BNodeElement> entries = current_node.getEntries();
        // ARGH JAVA AND YOUR LACK OF TRANSPARENT COMPARATORS
        while (true) {
            int pos = Collections.binarySearch(entries, new BNodeElement(key, 0L, 0L));
            res.addComponent(current_node);
            if (pos >= 0) {
                return res;
            } else {
                pos = -pos - 1; // get the "insertion point"
                assert pos != entries.size();
                long child = entries.get(pos).getLeftChild();
                if (child != BNode.not_a_child) {
                    current_node = access.lookup(child);
                    entries = current_node.getEntries();
                } else {
                    break;
                }
            }
        }
        return res;
    }

    public static void main(String[] args) {
        if (true) {
            BTree t = testDataBasic();
            System.out.println(t.resolve(15));
            System.out.println(t.resolve(5));
            System.out.println(t.resolve(40));
            System.out.println(t.resolve(50));
            System.out.println(t.resolve(0));
            System.out.println(t.resolve(0));
            System.out.println();
            System.out.println(t);
        }
        if (true) {
            BTree t = testDataEmpty();
            for (long i = 1; i <= 2 * BNode.d + 1; ++i) {
                t.insert(i, "a");
                System.out.println(t);
            }
        }
    }

    @Override
    public void close() throws Exception {
        access.close();
    }

    private boolean tryCompensate(BNode parent, BNode left, BNode right, Stream<BNode.BNodeElement> elements, int parent_index) {
        List<BNode.BNodeElement> list = elements.sorted().collect(Collectors.toList());
        if (list.size() > BNode.d * 4) {
            return false;
        }
        {
            List<BNode.BNodeElement> entries = list.subList(0, list.size() / 2);
            left = new BNode(entries, left.getThisAddress());
            access.replace(left.getThisAddress(), left);
        }
        {
            List<BNode.BNodeElement> entries = list.subList(list.size() / 2 + 1, list.size());
            right = new BNode(entries, right.getThisAddress());
            access.replace(right.getThisAddress(), right);
        }
        {
            BNode.BNodeElement el = list.get(list.size() / 2);
            List<BNode.BNodeElement> entries = new ArrayList<>(parent.getEntries());
            entries.set(parent_index, el);
            parent = new BNode(entries, parent.getThisAddress());
            access.replace(parent.getThisAddress(), parent);
        }
        return true;
    }

    private boolean tryCompensateInsert(
            BNode parent,
            BNode left,
            BNode right,
            List<BNode.BNodeElement> parent_entries,
            BNode.BNodeElement new_element,
            int parent_index) {
        Stream<BNodeElement> elements
                = Stream.of(
                        Stream.of(parent_entries.get(parent_index), new_element),
                        left.getEntries().stream(),
                        right.getEntries().stream())
                .reduce(Stream.empty(), Stream::concat);
        return tryCompensate(parent, left, right, elements, parent_index);
    }

    private boolean tryCompensateInsertBoth(BNode parent, BNode child, BNode.BNodeElement new_element) {
        List<BNode.BNodeElement> parent_entries = parent.getEntries();
        int parent_index = parent_entries.lastIndexOf(child);
        if (parent_index > 0) {
            if (tryCompensateInsert(
                    parent,
                    access.lookup(parent_entries.get(parent_index - 1).getLeftChild()),
                    child,
                    parent_entries,
                    new_element,
                    parent_index)) {
                return true;
            }
        }
        if (parent_index < parent_entries.size() - 1) {
            if (tryCompensateInsert(
                    parent,
                    child,
                    access.lookup(parent_entries.get(parent_index + 1).getLeftChild()),
                    parent_entries,
                    new_element,
                    parent_index)) {
                return true;
            }
        }
        return false;
    }

    private void insertImpl(long key, long value_address, BPath path) {
        BNode node = path.lastComponent();
        List<BNode.BNodeElement> entries = node.getEntries();
        // just put it
        if (entries.size() < BNode.d * 2) {
            long address = node.getThisAddress();
            node = new BNode(
                    Stream
                    .concat(entries.stream(), Stream.of(new BNode.BNodeElement(key, value_address, BNode.not_a_child)))
                    .collect(Collectors.toList()), address);
            access.replace(address, node);
            return;
        }
        // compensation
        // currently broken as fuck
        if (path.hasParent()) {
            BNode.BNodeElement el = new BNode.BNodeElement(key, value_address, BNode.not_a_child);
            if (tryCompensateInsertBoth(path.getParent(), path.lastComponent(), el)) {
                return;
            }
        }
        // splitting
    }

    public String insert(long key, String value) {
        if (key == BNode.not_a_key) {
            throw new IllegalArgumentException("sentinel added");
        }
        long allocated_value_address = 0L;
        BPath path = resolve(key);
        insertImpl(key, allocated_value_address, path);
        return value;
    }

    public void delete(long key) {
        if (key == BNode.not_a_key) {
            throw new IllegalArgumentException("trying to remove sentinel");
        }
        BPath path = resolve(key);
        
    }

    private void traversal(long rn, Consumer<BNode> f) {
        BNode n = access.lookup(rn);
        f.accept(n);
        n.getEntries().stream()
                .map(el -> el.getLeftChild())
                .filter(childkey -> childkey != BNode.not_a_child)
                .forEach(childkey -> {
                    traversal(childkey, f);
                });
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("Struktura drzewa:\n");
        traversal(root, x -> {
            b.append(x.getThisAddress()).append(": ");
            for (BNode.BNodeElement el : x.getEntries()) {
                b.append(el.getLeftChild());
                if (el.getKey() != BNode.not_a_key) {
                    b.append("(").append(el.getKey()).append(")");
                }
            }
            b.append("\n");
        });
        b.append("Free lista:\n");
        if (free_list_nodes != BNode.not_a_child) {
            for (BNode current_node = access.lookup(free_list_nodes);
                    current_node.getSentinel().getValuePosition() != BNode.not_a_child;
                    current_node = access.lookup(current_node.getSentinel().getValuePosition())) {
                b.append(current_node.getThisAddress());
                b.append("->");
            }
        }
        b.append("\n");
        return b.toString();
    }
}
