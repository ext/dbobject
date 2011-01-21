package com.sidvind.dbobject;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author David Sveningsson <david.sveningsson@bth.se>
 *
 */
public class TupleList {
	public class Tuple implements Map.Entry<String, Object> {
		public String name;
		public Object value;
		
		public Tuple(String name, Object value){
			this.name = name;
			this.value = value;
		}
		
		@Override
		public String getKey() {
			return name;
		}

		@Override
		public Object getValue() {
			return value;
		}

		@Override
		public Object setValue(Object value) {
			Object old = this.value;
			this.value = value;
			return old;
		}
	}
	
	private List<Tuple> list = new ArrayList<Tuple>();
	
	public void add(String name, Object value){
		list.add(new Tuple(name, value));
	}

	public Set<Tuple> entrySet(){
		return new AbstractSet<Tuple>(){
			@Override
			public Iterator<Tuple> iterator() {
				return list.iterator();
			}

			@Override
			public int size() {
				return list.size();
			}
		};
	}
}