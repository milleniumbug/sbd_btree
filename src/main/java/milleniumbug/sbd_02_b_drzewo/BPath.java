package milleniumbug.sbd_02_b_drzewo;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

public final class BPath implements Cloneable {
    private final Deque<BNode> path = new ArrayDeque<>();
    
    public void addComponent(BNode node)
    {
        path.addFirst(node);
    }
    
    public BNode lastComponent()
    {
        return path.getFirst();
    }
    
    public void removeComponent()
    {
        path.removeFirst();
    }
    
    public boolean hasParent()
    {
        return path.size() > 1;
    }
    
    public BNode getParent()
    {
        BNode top = lastComponent();
        removeComponent();
        BNode ret = lastComponent();
        addComponent(top);
        return ret;
    }
    
    public void replaceComponent(BNode node)
    {
        removeComponent();
        addComponent(node);
    }
    
    public BPath()
    {
        
    }
    
    public BPath(BPath other)
    {
        this();
        path.addAll(other.path);
    }

    @Override
    public String toString() {
        List<String> l = path.stream()
                .mapToLong(x -> x.getThisAddress())
                .mapToObj(Long::toString)
                .collect(Collectors.toList());
        Collections.reverse(l);
        String pathstr = l.stream().collect(Collectors.joining("/"));
        return "BPath{" + pathstr + '}';
    }
}
