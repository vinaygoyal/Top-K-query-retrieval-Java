package indexing;

import java.util.ArrayList;

public class BTree {
	public BTreeNode root;
	int blockSize;
	
	public BTree(int _blockSize) {
		root = null;
		blockSize = _blockSize;
	}
	
	//search in the binary tree
	public ArrayList<Integer> search(Integer key) {
		return root.search(key);
	}
	
	//insert into the tree
	public void insert(int key, int value) {

		//if tree has no root, create one root and set it as leaf
		if(root == null) {
			root = new BTreeNode(blockSize, true);
			root.keys[0] = new Entry(key, value);
			root.numKeys++;
			return;
		}

		//first find if there exists a key
		// if tree isn't empty
		ArrayList<Integer> xxx = root.search(key);
		if(xxx.size() != 0) {
			xxx.add(value);
			return;
		}
		
		if(root.numKeys == (2*blockSize-1)) { 
			
			// create a new node which isn't a leaf. this will be the new parent
			BTreeNode newRoot = new BTreeNode(blockSize, false);
			
			// set current root as child of newRoot
			BTreeNode oldRoot = root;
			newRoot.children[0] = oldRoot;
			newRoot.numChilds++;
			
			// split oldRoot and attach to newRoot
			oldRoot.split(0, newRoot); 
			root = newRoot;

			// now find where the key should be added and insert accordingly
			Integer ind = 0;
			if(root.keys[ind].getKey() < key) {
				++ind;
			}
			root.children[ind].insertNonFull(key, value);
		}
		else {
			root.insertNonFull(key, value);
		}
	}
	
	//traverse into the tree
	void traverse() {
		root.traverse();
	}
	
	public void traverseSequentiallyReverse() {
		BTreeNode rightmostLeaf = root.reachRightmostLeaf();
		rightmostLeaf.traverseSequentiallyReverse();
	}
	
	public Entry traverseReverse(int rows) {
		BTreeNode rightmostLeaf = root.reachRightmostLeaf();
		return rightmostLeaf.traverseReverse(rows);
	}
}

