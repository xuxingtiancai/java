package main;

import java.util.Stack;

import util.MinFunc;


//O(1)求最小值的栈
public class M21_MinStack<E> extends Stack<E>{
	//构造函数
	public M21_MinStack(MinFunc<E> minFunc) {
		super();
		this.minFunc = minFunc;
		this.minStack = new Stack<E>();
	}
	
	//成员变量
	private Stack<E> minStack;
	private MinFunc<E> minFunc;
	
	//主要函数
	public E push(E e) {
		super.push(e);
		if(minStack.isEmpty()) {
			minStack.push(e);
		} else {
			minStack.push(minFunc.min(e, minStack.peek()));
		}
		return e;
	}
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
