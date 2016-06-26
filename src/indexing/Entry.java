package indexing;

import java.util.ArrayList;

public class Entry {
		private int _key;
		private ArrayList<Integer> _value = new ArrayList<Integer>();
		
		public Entry(int key, int value) {
			_key = key;
			_value.add(value);
		}
		
		public int getKey() {
			return _key;
		}	
		
		public ArrayList<Integer> getValue() {
			return _value;
		}
		
		public void addValue(Integer val) {
			_value.add(val);
		}
}

