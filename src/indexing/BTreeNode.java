package indexing;

import java.util.ArrayList;

public class BTreeNode {
	
	// for n children, there can be only n-1 keys
	private Integer blockSize;
	
	public Entry[] keys; // max : 2*blockSize -1
	public BTreeNode[] children; // max : 2*blockSize
	BTreeNode left, right;

	
	private boolean isLeaf; // is it a leaf node or not
	
	public int numKeys;
	public int numChilds;
	
	//numKeys is the number of keys we want
	public BTreeNode(Integer bSize, boolean _isLeaf) {
		numKeys = 0;
		blockSize = bSize;
		keys = new Entry[2*bSize - 1];
		children = new BTreeNode[2*bSize];
		isLeaf = _isLeaf;
	}

	public boolean isLeaf() { return isLeaf; }
	
	public boolean insertNonFull(Integer key, Integer value) {
		Integer ind = this.numKeys-1;
		
		if(this.isLeaf() == true) {
			// this involves shifting keys
			
			// else shift keys and create a new pair and add
			while(ind >= 0 && this.keys[ind].getKey() > key) {
				// shift keys
				this.keys[ind+1] = this.keys[ind];
				ind--;
			}
			this.keys[ind+1] = new Entry(key, value);
			this.numKeys++;
		}
		else {
			// if this isn't a leaf node
			// find the children where it should be inserted
			while(ind >= 0 && this.keys[ind].getKey() > key) {
				ind--;
			}
			
			//check for duplicate
			BTreeNode child = this.children[ind+1];
			
			if(child.numKeys == (2*blockSize-1)) {
				
				// split child (oldRoot) and attach to this (parent of child)
				// above split call creates a newNode which is generally the right node. 
				// so, it needs to be linked to the parent
					
				child.split(ind+1, this); 
					
				if(this.keys[ind+1].getKey() < key) {
					ind++;
				}
			}
			child = this.children[ind+1];
			child.insertNonFull(key, value);
		}
		return false;
	}
	
	public void split(Integer index, BTreeNode newRoot) {
		// 'this' is to be split and attached to newRoot
		
		BTreeNode secondNode = new BTreeNode(blockSize, this.isLeaf());
		
		// blockSize to 2*blockSize - 1 --> push into secondNode
		// move both keys and children
		for(Integer j = 0; j < blockSize-1; ++j) { 
			secondNode.keys[j] = this.keys[j+blockSize];
			secondNode.numKeys++;
			
			this.keys[j+blockSize] = null;
			this.numKeys--;
		}
		
		// here this is my child node which is being split
		// if some intermediate node, move the children as well
		if(this.isLeaf() == false) { 
			for(int j = 0; j < blockSize; ++j) {
				secondNode.children[j] = this.children[j+blockSize];
				secondNode.numChilds++;
				this.children[j+blockSize] = null;
				this.numChilds--;
			}
		}
		
		// added to secondNode and removed from oldNode (this)	
		this.right = secondNode;
		secondNode.left = this;
		
		for(int i = newRoot.numChilds-1; i >= index+1; --i) {
			newRoot.children[i+1] = newRoot.children[i];
		}
		newRoot.children[index+1] = secondNode; // index was set in the calling function to second Node.
		newRoot.numChilds++;

		for(int i = newRoot.numKeys-1; i >= index; --i) {
			newRoot.keys[i+1] = newRoot.keys[i];
		}
		newRoot.keys[index] = this.keys[blockSize -1]; // middle as the key in the newNode
		newRoot.numKeys++;
	}
	
	public ArrayList<Integer> search(Integer key) {
		//to search, we need to go to the leaf for sure
		if (this.isLeaf) {
			// if it's a leaf node return null
			for(int i = 0; i < this.numKeys; ++i) {
				if (this.keys[i].getKey() != key) {
					continue;
				}			
				return this.keys[i].getValue();
			}
			return new ArrayList<Integer>();
		}
		
		int index = this.numKeys-1;
		while(index >= 0 && this.keys[index].getKey() >= key) {
			index--;
		}

		return this.children[index+1].search(key);
	}
	
	public void traverse() {
		int i;
		for(i = this.numKeys-1; i >=0; --i) {
			if(this.isLeaf() == false) {
				this.children[i].traverse();
			}
			System.out.println(this.keys[i].getKey());
		}
		
		if (this.isLeaf() == false) {
			this.children[i-1].traverse();
		}
	}
	
	public BTreeNode reachRightmostLeaf() {
		
		for(int i = this.numKeys; i >= 0; --i) {
			if(this.isLeaf() == false) {
				return this.children[i].reachRightmostLeaf();
			}
			else {
				break;
			}
		}
		return this;
	}
	
	public void traverseSequentiallyReverse() {
		for(int i = this.numKeys-1; i >=0; --i) {
			System.out.println(this.keys[i].getKey());
		}
		if(this.left != null) {
			this.left.traverseSequentiallyReverse();
		}
	}
	
	public Entry traverseReverse(int rows) {
		for(int i = this.numKeys-1; i >= 0; --i) {
			rows--;
			if(rows == 0) {
				return this.keys[i];
			}
		}
		return this.traverseReverse(rows);
	}
}


