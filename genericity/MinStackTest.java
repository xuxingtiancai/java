package algorithm;

import java.util.EmptyStackException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;


public class M21_MinStackTest{
	private M21_MinStack<Integer> stack;//提取公用的对象 
	
	@Before
	public void setUp() {//初始化公用对象 
		MinFunc<Integer> func = new MinFunc<Integer>() {
			@Override
			public Integer min(Integer e1, Integer e2) {
				if(e1 < e2) {
					return e1;
				}
				return e2;
			}
		};
		stack = 
			new M21_MinStack<Integer>(func);
	}  
	
	@Test
	public void testNormal() {
		stack.push(3);
		//insert > min
		stack.push(4);
		Assert.assertEquals(3, stack.min().intValue());
		
		//insert < min
		stack.push(2);
		Assert.assertEquals(2, stack.min().intValue());
		
		//pop != min
		stack.push(5);
		Assert.assertEquals(5, stack.pop().intValue());
		Assert.assertEquals(2, stack.min().intValue());
		
		//pop == min
		Assert.assertEquals(2, stack.pop().intValue());
		Assert.assertEquals(3, stack.min().intValue());
	} 
	
	@Test
	public void testNull() {
		Assert.assertNull(stack.popSafe());
		Assert.assertNull(stack.minSafe());
	}
	
	@Test(expected = EmptyStackException.class)
	public void testException(){
		stack.pop();
	}
}
