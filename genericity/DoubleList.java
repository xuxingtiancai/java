class EnumValue implements Comparable<EnumValue>{
	int index;
	Integer val;
	public EnumValue(int index, int val) {
        this.index = index;
        this.val = val;
    }
	@Override
	public int compareTo(EnumValue o) {
		return val.compareTo(o.val);
	}
}
class ListNode<T extends Comparable<? super T>> implements Comparable<ListNode<T>>{
	T val;
	ListNode<T> pre = null;
	ListNode<T> next = null;
	public ListNode(T val) {
		this.val = val;
		this.pre = null;
		this.next = null;
	}
	@Override
	public int compareTo(ListNode<T> o) {
		return val.compareTo(o.val);
	}
	
}

//from LRU cache
class DoubleList<T extends Comparable<? super T>> {
	ListNode<T> head;
	ListNode<T> tail;
    
    public DoubleList() {
        this.head = null;
        this.tail = null;
    }
    
	public ListNode<T> remove(ListNode<T> node) {
        if(node == null)
            return null;
        if(node.pre == null && node.next == null) {
            this.head = null;
            this.tail = null;
            return node;
        }
        if(node.pre == null) {
            node.next.pre = null;
            this.head = node.next;
            return node;
        }
        if(node.next == null) {
            node.pre.next = null;
            this.tail = node.pre;
            return node;
        }
        node.pre.next = node.next;
        node.next.pre = node.pre;
        node.pre = node.next = null;
        return node;
    }
    
    public void append(ListNode<T> node) {
        node.pre = node.next = null;
        if(this.tail == null) {
            this.head = node;
            this.tail = node;
            return;
        }
        this.tail.next = node;
        node.pre = this.tail;
        this.tail = node;
    }
        
    public void update(ListNode<T> node) {
        this.remove(node);
        this.append(node);
    }
    
    public ListNode<T> pop() {
        return this.remove(this.head);
    }
}

public class Solution {
    public int maxArea(int[] height) {
        ArrayList<ListNode<EnumValue>> queue = new ArrayList<ListNode<EnumValue>>();
        for(int i = 0; i < height.length; i++) {
        	queue.add(new ListNode<EnumValue>(new EnumValue(i, height[i])));
        }
        
        //doubleList
        DoubleList<EnumValue> doubleList = new DoubleList<EnumValue>();
        for(ListNode<EnumValue> node : queue) {
            doubleList.append(node);
        }
        
        //sort
        Collections.sort(queue);
        
        //totalMax;
        int totalMax = 0;
        for(int i = 0; i < queue.size() - 1; i++) {
            ListNode<EnumValue> node = queue.get(i);
            doubleList.remove(node);
            totalMax = Math.max(Math.max(totalMax, node.val.val * Math.abs(node.val.index - doubleList.head.val.index)), node.val.val * Math.abs(node.val.index - doubleList.tail.val.index));
        }    
        return totalMax;
    }
}
