package milleniumbug.sbd_02_b_drzewo;

import java.util.ArrayList;
import java.util.Collections;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import lombok.Getter;

public class BNode {

    public static final int d = 170 / 2;
    public static final long not_a_key = Long.MAX_VALUE;
    public static final long not_a_child = 0;

    @Getter
    private List<BNodeElement> entries;
    @Getter
    private long thisAddress;
    
    public static BNode freeNode(long nextAddress, long thisAddress)
    {
        return new BNode(Collections.singletonList(new BNodeElement(not_a_key, nextAddress, not_a_child)), thisAddress);
    }

    public BNode(List<BNodeElement> entries, long thisAddress) {
        if (entries.size() > 2 * d) {
            throw new IllegalArgumentException("more than 2*d entries");
        }
        entries = new ArrayList<>(entries);
        entries.sort((x, y) -> x.compareTo(y));
        this.entries = unmodifiableList(entries);
        BNodeElement last = getSentinel();
        if(last.getKey() != BNode.not_a_key)
            throw new IllegalArgumentException("no sentinel value at the end");
        this.thisAddress = thisAddress;
    }
    
    public boolean isLeaf()
    {
        return entries.stream().allMatch(x -> x.getLeftChild() == BNode.not_a_child);
    }
    
    public final BNodeElement getSentinel()
    {
        return entries.get(entries.size()-1);
    }
    
    public final List<BNodeElement> getEntriesNoSentinel()
    {
        return entries.subList(0, entries.size()-1);
    }

    public static class BNodeElement implements Comparable<BNodeElement> {

        @Getter
        private final long key;
        @Getter
        private final long valuePosition;
        @Getter
        private final long leftChild;


        public BNodeElement(long key, long value_pos, long child) {
            this.key = key;
            this.valuePosition = value_pos;
            this.leftChild = child;
        }

        @Override
        public int compareTo(BNodeElement t) {
            return Long.compare(this.key, t.key);
        }

    }
}
