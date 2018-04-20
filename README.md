# README
job Scheduler using minHeap and RedBlack tree


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Scheduler {

	public final static String INSERT = "Insert";
	public final static String PRINT_JOB = "PrintJob";
	public final static String NEXT_JOB = "NextJob";
	public final static String PREVIOUS_JOB = "PreviousJob";
	public static MinHeap<Job> minHeap = new MinHeap<Job>();
	public static RedBlackTree rbt = new RedBlackTree();
	public static int[] sortedArray = new int[100];
	public static int arrayIndex = 0;
	public static int totalRunTime = 0;
	public static Job runningJob = null;

	

	public static void main(String[] args) throws Exception {

		FileReader fileReader = null;
		try {
			File file = new File(args[0]);
			fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			
			//Start for loop in this method
			startScheduler(bufferedReader);
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			fileReader.close();
		}

	}


	/**
	 * Start scheduler here
	 * @param bufferedReader
	 * @throws Exception
	 */
	private static void startScheduler(BufferedReader bufferedReader)
			throws Exception {
		int count = 0;
		int timeToRun = -1;
		String line = null;

		for (int i = 0; i >= 0; i++) {
			if (timeToRun == -1) {
				if ((line = bufferedReader.readLine()) != null) {
					timeToRun = getStartTime(line);
				}
			}

			if (timeToRun == i && line != null) {
				processInput(line);
				timeToRun = -1;
			}

			if (runningJob != null) {
				int t = runningJob.getExecutedTime();
				t++;
				runningJob.setExecutedTime(t);
				if (t >= runningJob.getTotalTime()) {
					runningJob.setExecutedTime(runningJob.getTotalTime());
					runningJob = null;
				}

			}
			count++;
			if (count == 5) {
				if (runningJob != null
						&& (runningJob.getExecutedTime() < runningJob
								.getTotalTime())) {
					rbt.removeJob(runningJob.getJobId());
					rbt.insertJob(runningJob);
					minHeap.insert(runningJob);
				}
				count = 0;
				runningJob = minHeap.extractMin();
			}
			if (timeToRun == -1 && runningJob == null && line == null) {
				break;
			}
		}

	}

	/**
	 * return start time of the command 
	 * example: 49950: Insert(19,55000) -> return 49950
	 * @param line
	 * @return
	 * @throws Exception
	 */
	private static int getStartTime(String line) throws Exception {
		String[] s;
		int time = -1;
		line = line.trim();
		if (line.contains(":")) {
			s = line.split(":");
			time = Integer.parseInt(s[0]);
		} else {
			if (line.length() > 1)
				throw new Exception("Invalid Input:" + line);
		}
		return time;
	}
	
	
	/**
	 * Add a job to the scheduler if time arrived
	 * @param j
	 */
	private static void addJobToScheduler(Job j) {
		if (runningJob != null) {
			rbt.removeJob(runningJob.getJobId());
			rbt.insertJob(runningJob);
			minHeap.insert(runningJob);
			runningJob = j;
		} else {
			runningJob = minHeap.extractMin();
		}
	}


	/**
	 * After reading a line from the file, process with with given commands
	 * supported commands: "Print, Insert, PrintJob, PreviousJob"
	 * @param command
	 * @param str
	 * @throws Exception
	 */
	private static void executeCommand(String command, String str)
			throws Exception {
		String[] s;
		int jobId = 0;
		int scheduleTime = 0;

		str = str.trim();
		command = command.trim();
		if (str.charAt(str.length() - 1) == ')') {
			str = (String) str.substring(0, str.length() - 1);
		} else {
			throw new Exception("Invalid Input");
		}

		s = str.split("\\,");

		if (command.equals(INSERT)) {
			jobId = Integer.parseInt(s[0]);
			scheduleTime = Integer.parseInt(s[1]);

			Job j = createJob(jobId, 0, scheduleTime);
			addJobToScheduler(j);

			// System.out.println(command + ": " + j);

		} else if (command.equals(PRINT_JOB)) {

			int runningJobId = -1;
			boolean isPrintRunningJob = false;
			if (runningJob != null) {
				runningJobId = runningJob.getJobId();
				isPrintRunningJob = true;
			}
			// for (int i = 0; i < s.length; i++) {
			if (s.length > 1) {
				boolean isFound = false;
				int sjid = Integer.parseInt(s[0]);
				int ejid = Integer.parseInt(s[1]);
				for (int i = sjid + 1; i < ejid; i++) {

					Job sJob = findScheduledJob(i);
					if (sJob != null
							&& sJob.getExecutedTime() < sJob.getTotalTime()) {
						if (isFound) {
							System.out.print(",");
						}
						System.out.print("(" + sJob.getJobId() + ","
								+ sJob.getExecutedTime() + ","
								+ sJob.getTotalTime() + ")");
						isFound = true;
					}
				}
				if (!isFound) {
					System.out.print("(0,0,0)");
				}
			} else {
				jobId = Integer.parseInt(s[0]);
				Job sJob = findScheduledJob(jobId);
				if (sJob != null
						&& sJob.getExecutedTime() < sJob.getTotalTime()) {
					System.out.print("(" + sJob.getJobId() + ","
							+ sJob.getExecutedTime() + ","
							+ sJob.getTotalTime() + ")");
				} else {
					System.out.print("(0,0,0)");
				}
				if (runningJobId == jobId) {
					isPrintRunningJob = false;
				}
			}
			System.out.println();

		} else if (command.equals(NEXT_JOB)) {
			jobId = Integer.parseInt(str);

			Integer higherJobId = searchArray(jobId, true, false);
			if (higherJobId == -1) {
				System.out.println("(0,0,0)");
			} else {
				Job sJob = rbt.findJob(higherJobId);
				System.out.println("(" + sJob.getJobId() + ","
						+ sJob.getExecutedTime() + "," + sJob.getTotalTime()
						+ ")");
			}

		} else if (command.equals(PREVIOUS_JOB)) {
			jobId = Integer.parseInt(str);
			Integer lowerJobId = searchArray(jobId, false, true);
			if (lowerJobId == -1) {
				System.out.println("(0,0,0)");
			} else {
				Job sJob = rbt.findJob(lowerJobId);
				System.out.println("(" + sJob.getJobId() + ","
						+ sJob.getExecutedTime() + "," + sJob.getTotalTime()
						+ ")");
			}
		} else {
			throw new Exception("Invalid Input: " + command);
		}
	}

	private static void processLine(String line) throws Exception {
		String command = null;
		String[] ss;
		ss = line.split("\\(");
		command = ss[0];
		executeCommand(command, ss[1]);
	}

	private static void processInput(String line) throws Exception {
		String[] s;
		int time = 0;
		line = line.trim();
		s = line.split(":");
		processLine(s[1]);

	}
	private static int searchArray(int target, boolean next, boolean prev) {
		int indx = -1;
		for (int i = 0; i < arrayIndex; i++) {
			if (sortedArray[i] == target) {
				indx = i;
				break;
			}
		}
		if (next) {
			if (indx < arrayIndex-1) {
				indx++;
				return sortedArray[indx];
			}
		}
		if (prev) {
			if (indx > 0) {
				indx--;
				return sortedArray[indx];
			}
		}
		return -1;
	}

	private static void sortArray() {
		int temp = 0;
		for (int i = 0; i < arrayIndex; i++) {
			for (int j = 1; j < (arrayIndex-1); j++) {
				if (sortedArray[j - 1] > sortedArray[j]) {
					temp = sortedArray[j - 1];
					sortedArray[j - 1] = sortedArray[j];
					sortedArray[j] = temp;
				}
			}
		}
	}

	private static Job createJob(int jobId, int executedTime, int totalTime) {
		Job job = new Job(jobId, executedTime, totalTime);
		minHeap.insert(job);
		rbt.insertJob(job);
		sortedArray[arrayIndex] = job.getJobId();
		arrayIndex++;
		sortArray();
		return job;
	}

	private static Job findScheduledJob(int jobId) {
		return rbt.findJob(jobId);
	}

}

class Job implements Comparable<Job> {
	int jobId;
	int totalTime;
	int executedTime;

	public Job(int jobId, int executedTime, int totalTime) {
		this.jobId = jobId;
		this.executedTime = executedTime;
		this.totalTime = totalTime;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public int getTotalTime() {
		return totalTime;
	}

	public void setTotalTime(int totalTime) {
		this.totalTime = totalTime;
	}

	public int getExecutedTime() {
		return executedTime;
	}

	public void setExecutedTime(int executedTime) {
		this.executedTime = executedTime;
	}

	public int compareTo(Job o) {
		return this.executedTime - o.executedTime;
	}

	@Override
	public String toString() {
		return "Job [jobId=" + jobId + ", executedTime=" + executedTime
				+ ", totalTime=" + totalTime + "]";
	}

}

class MinHeap<T extends Comparable<T>> {
	private ArrayList<T> heap;

	/**
	 * Constructor
	 */
	public MinHeap() {
		heap = new ArrayList<T>();
	}

	/**
	 * Return the element with the minimum key, and remove it from the queue.
	 * 
	 * @return the element with the minimum key, or null if queue empty.
	 */
	public T extractMin() {
		if (heap.size() <= 0)
			return null;
		else {
			T minVal = heap.get(0);
			heap.set(0, heap.get(heap.size() - 1)); // Move last to position 0
			heap.remove(heap.size() - 1);
			minHeapify(heap, 0);
			return minVal;
		}
	}

	/**
	 * Insert an element into the priority queue. Keep in heap order
	 * 
	 * @param element
	 *            the element to insert
	 */
	public void insert(T element) {
		heap.add(element); // Put new value at end;
		int loc = heap.size() - 1; // and get its location

		// Swap with parent until parent not larger
		while (loc > 0 && heap.get(loc).compareTo(heap.get(parent(loc))) < 0) {
			swap(heap, loc, parent(loc));
			loc = parent(loc);
		}
	}

	/**
	 * Is the priority queue empty?
	 * 
	 * @return true if the queue is empty, false if not empty.
	 */
	public boolean isEmpty() {
		return heap.size() == 0;
	}

	/**
	 * Return the element with the minimum key, without removing it from the
	 * queue.
	 * 
	 * @return the element with the minimum key, or null if queue empty.
	 */
	public T minimum() {
		if (heap.size() <= 0)
			return null;
		else
			return heap.get(0);
	}

	/**
	 * Restore the min-heap property. When this method is called, the min-heap
	 * property holds everywhere, except possibly at node i and its children.
	 * When this method returns, the min-heap property holds everywhere.
	 * 
	 * @param a
	 *            the list to sort
	 * @param i
	 *            the position of the possibly bad spot in the heap
	 */
	private static <T extends Comparable<T>> void minHeapify(ArrayList<T> a,
			int i) {
		int left = leftChild(i); // index of node i's left child
		int right = rightChild(i); // index of node i's right child
		int smallest; // will hold the index of the node with the smallest
						// element
		// among node i, left, and right

		// Is there a left child and, if so, does the left child have an
		// element smaller than node i?
		if (left <= a.size() - 1 && a.get(left).compareTo(a.get(i)) < 0)
			smallest = left; // yes, so the left child is the largest so far
		else
			smallest = i; // no, so node i is the smallest so far

		// Is there a right child and, if so, does the right child have an
		// element smaller than the larger of node i and the left child?
		if (right <= a.size() - 1
				&& a.get(right).compareTo(a.get(smallest)) < 0)
			smallest = right; // yes, so the right child is the largest

		// If node i holds an element smaller than both the left and right
		// children, then the max-heap property already held, and we need do
		// nothing more. Otherwise, we need to swap node i with the larger
		// of the two children, and then recurse down the heap from the larger
		// child.
		if (smallest != i) {
			swap(a, i, smallest);
			minHeapify(a, smallest);
		}
	}

	/**
	 * Swap two locations i and j in ArrayList a.
	 * 
	 * @param a
	 *            the arrayList
	 * @param i
	 *            first position
	 * @param j
	 *            second position
	 */
	private static <T> void swap(ArrayList<T> a, int i, int j) {
		T t = a.get(i);
		a.set(i, a.get(j));
		a.set(j, t);
	}

	/**
	 * Return the index of the left child of node i.
	 * 
	 * @param i
	 *            index of the parent node
	 * @return index of the left child of node i
	 */
	private static int leftChild(int i) {
		return 2 * i + 1;
	}

	/**
	 * Return the index of the right child of node i.
	 * 
	 * @param i
	 *            index of parent
	 * @return the index of the right child of node i
	 */
	private static int rightChild(int i) {
		return 2 * i + 2;
	}

	/**
	 * Return the index of the parent of node i (Parent of root will be -1)
	 * 
	 * @param i
	 *            index of the child
	 * @return index of the parent of node i
	 */
	private static int parent(int i) {
		return (i - 1) / 2;
	}
}

class RedBlackTree {

	private final int RED = 0;
	private final int BLACK = 1;

	public class Node {

		int key = -1, color = BLACK;
		Node left = nil, right = nil, parent = nil;
		Job job = null;

		Node(int key) {
			this.key = key;
		}

		Node(int key, Job job) {
			this.key = key;
			this.job = job;
		}
	}

	private final Node nil = new Node(-1, null);
	private Node root = nil;

	public void printTree(Node node) {
		if (node == nil) {
			return;
		}
		printTree(node.left);
		System.out
				.print(((node.color == RED) ? "Color: Red " : "Color: Black ")
						+ "Key: " + node.key + " Parent: " + node.parent.key
						+ "\n");
		printTree(node.right);
	}

	private Node findNode(Node findNode, Node node) {
		if (root == nil) {
			return null;
		}

		if (findNode.key < node.key) {
			if (node.left != nil) {
				return findNode(findNode, node.left);
			}
		} else if (findNode.key > node.key) {
			if (node.right != nil) {
				return findNode(findNode, node.right);
			}
		} else if (findNode.key == node.key) {
			return node;
		}
		return null;
	}

	private void insert(Node node) {
		Node temp = root;
		if (root == nil) {
			root = node;
			node.color = BLACK;
			node.parent = nil;
		} else {
			node.color = RED;
			while (true) {
				if (node.key < temp.key) {
					if (temp.left == nil) {
						temp.left = node;
						node.parent = temp;
						break;
					} else {
						temp = temp.left;
					}
				} else if (node.key >= temp.key) {
					if (temp.right == nil) {
						temp.right = node;
						node.parent = temp;
						break;
					} else {
						temp = temp.right;
					}
				}
			}
			fixTree(node);
		}
	}

	// Takes as argument the newly inserted node
	private void fixTree(Node node) {
		while (node.parent.color == RED) {
			Node uncle = nil;
			if (node.parent == node.parent.parent.left) {
				uncle = node.parent.parent.right;

				if (uncle != nil && uncle.color == RED) {
					node.parent.color = BLACK;
					uncle.color = BLACK;
					node.parent.parent.color = RED;
					node = node.parent.parent;
					continue;
				}
				if (node == node.parent.right) {
					// Double rotation needed
					node = node.parent;
					rotateLeft(node);
				}
				node.parent.color = BLACK;
				node.parent.parent.color = RED;
				// if the "else if" code hasn't executed, this
				// is a case where we only need a single rotation
				rotateRight(node.parent.parent);
			} else {
				uncle = node.parent.parent.left;
				if (uncle != nil && uncle.color == RED) {
					node.parent.color = BLACK;
					uncle.color = BLACK;
					node.parent.parent.color = RED;
					node = node.parent.parent;
					continue;
				}
				if (node == node.parent.left) {
					// Double rotation needed
					node = node.parent;
					rotateRight(node);
				}
				node.parent.color = BLACK;
				node.parent.parent.color = RED;
				// if the "else if" code hasn't executed, this
				// is a case where we only need a single rotation
				rotateLeft(node.parent.parent);
			}
		}
		root.color = BLACK;
	}

	void rotateLeft(Node node) {
		if (node.parent != nil) {
			if (node == node.parent.left) {
				node.parent.left = node.right;
			} else {
				node.parent.right = node.right;
			}
			node.right.parent = node.parent;
			node.parent = node.right;
			if (node.right.left != nil) {
				node.right.left.parent = node;
			}
			node.right = node.right.left;
			node.parent.left = node;
		} else {// Need to rotate root
			Node right = root.right;
			root.right = right.left;
			right.left.parent = root;
			root.parent = right;
			right.left = root;
			right.parent = nil;
			root = right;
		}
	}

	void rotateRight(Node node) {
		if (node.parent != nil) {
			if (node == node.parent.left) {
				node.parent.left = node.left;
			} else {
				node.parent.right = node.left;
			}

			node.left.parent = node.parent;
			node.parent = node.left;
			if (node.left.right != nil) {
				node.left.right.parent = node;
			}
			node.left = node.left.right;
			node.parent.right = node;
		} else {// Need to rotate root
			Node left = root.left;
			root.left = root.left.right;
			left.right.parent = root;
			root.parent = left;
			left.right = root;
			left.parent = nil;
			root = left;
		}
	}

	// Deletes whole tree
	void deleteTree() {
		root = nil;
	}

	// Deletion Code .

	// This operation doesn't care about the new Node's connections
	// with previous node's left and right. The caller has to take care
	// of that.
	void transplant(Node target, Node with) {
		if (target.parent == nil) {
			root = with;
		} else if (target == target.parent.left) {
			target.parent.left = with;
		} else
			target.parent.right = with;
		with.parent = target.parent;
	}

	public boolean delete(Node z) {
		if ((z = findNode(z, root)) == null)
			return false;
		Node x;
		Node y = z; // temporary reference y
		int y_original_color = y.color;

		if (z.left == nil) {
			x = z.right;
			transplant(z, z.right);
		} else if (z.right == nil) {
			x = z.left;
			transplant(z, z.left);
		} else {
			y = treeMinimum(z.right);
			y_original_color = y.color;
			x = y.right;
			if (y.parent == z)
				x.parent = y;
			else {
				transplant(y, y.right);
				y.right = z.right;
				y.right.parent = y;
			}
			transplant(z, y);
			y.left = z.left;
			y.left.parent = y;
			y.color = z.color;
		}
		if (y_original_color == BLACK)
			deleteFixup(x);
		return true;
	}

	void deleteFixup(Node x) {
		while (x != root && x.color == BLACK) {
			if (x == x.parent.left) {
				Node w = x.parent.right;
				if (w.color == RED) {
					w.color = BLACK;
					x.parent.color = RED;
					rotateLeft(x.parent);
					w = x.parent.right;
				}
				if (w.left.color == BLACK && w.right.color == BLACK) {
					w.color = RED;
					x = x.parent;
					continue;
				} else if (w.right.color == BLACK) {
					w.left.color = BLACK;
					w.color = RED;
					rotateRight(w);
					w = x.parent.right;
				}
				if (w.right.color == RED) {
					w.color = x.parent.color;
					x.parent.color = BLACK;
					w.right.color = BLACK;
					rotateLeft(x.parent);
					x = root;
				}
			} else {
				Node w = x.parent.left;
				if (w.color == RED) {
					w.color = BLACK;
					x.parent.color = RED;
					rotateRight(x.parent);
					w = x.parent.left;
				}
				if (w.right.color == BLACK && w.left.color == BLACK) {
					w.color = RED;
					x = x.parent;
					continue;
				} else if (w.left.color == BLACK) {
					w.right.color = BLACK;
					w.color = RED;
					rotateLeft(w);
					w = x.parent.left;
				}
				if (w.left.color == RED) {
					w.color = x.parent.color;
					x.parent.color = BLACK;
					w.left.color = BLACK;
					rotateRight(x.parent);
					x = root;
				}
			}
		}
		x.color = BLACK;
	}

	Node treeMinimum(Node subTreeRoot) {
		while (subTreeRoot.left != nil) {
			subTreeRoot = subTreeRoot.left;
		}
		return subTreeRoot;
	}

	public Node findNode(Node node) {
		return findNode(node, root);
	}

	public Job findJob(int jobId) {
		if (findNode(new Node(jobId)) == null) {
			return null;
		}
		return findNode(new Node(jobId)).job;
	}

	public void insertJob(Job j) {
		insert(new Node(j.getJobId(), j));
	}

	public boolean removeJob(int jobId) {
		return delete(new Node(jobId));
	}

}
