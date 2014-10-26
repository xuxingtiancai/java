package algorithm;

import java.util.Stack;

//用于求最小值的抽象泛型类
abstract class MinFunc<E> {
	public abstract E min(E e1, E e2);
}

/*
 desc: O(1)求最小值的栈
*/
public class M21_MinStack<E> extends Stack<E>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4315954595548770897L;
	
	//构造函数
	public M21_MinStack(MinFunc<E> minFunc) {
		super();
		this.minFunc = minFunc;
		this.minStack = new Stack<E>();
	}
	
	//成员变量
	private Stack<E> minStack;
	private MinFunc<E> minFunc;
	
	@Override
	public E push(E e) {
		super.push(e);
		if(minStack.isEmpty()) {
			minStack.push(e);
		} else {
			minStack.push(minFunc.min(e, minStack.peek()));
		}
		return e;
	}
	@Override
	public E pop() {
		minStack.pop();
		return super.pop();
	}
	
	public E min() {
		return minStack.peek();
	}
	
	//return null with empty case
	public E popSafe() {
		if(isEmpty()) {
			return null;
		}
		return pop();
	}
	public E minSafe() {
		if(isEmpty()) {
			return null;
		}
		return min();
	}
}
