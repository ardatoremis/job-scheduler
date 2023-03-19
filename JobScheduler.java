import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
/*
Name: Arda Toremis
Student No: 211401026
Email: atoremis@etu.edu.tr
*/

public class JobScheduler {

	/*
	 * MinHeap implementation.
	 */
    public class MinHeap{
    	
    	private ArrayList<Job> heap; // The list that we store heap elements in.
        private int size; // Variable used to store size of the heap.
        
        private static final int FRONT = 1; // Front of the heap.
        
        public MinHeap() {
        	this.size = 0;
        	this.heap = new ArrayList<Job>();
        	this.heap.add(0, new Job(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE));
        }
        
        /*
         * Returns the index of the parent
         * for the node currently at given
         * index.
         */
        private int parent(int index) { 
        	return index / 2;
        }

        /*
         * Returns the index of left child
         * for the node currently at given
         * index.
         */
        private int leftChild(int index) { 
        	return (2 * index); 
        }
        
        /*
         * Returns the index of the right child
         * for the node currently at given
         * index.
         */
        private int rightChild(int index) {
            return (2 * index) + 1;
        }
        
        /*
         * Returns true if the node at given
         * index is leaf, false otherwise.
         */
        private boolean isLeaf(int index) {
     
            if (index > (this.size / 2)) {
                return true;
            }
     
            return false;
        }
        
        /*
         * Swaps the nodes at given indexes.
         */
        private void swap(int firstIndex, int secondIndex) {
     
            Job temp;
            temp = heap.get(firstIndex);
     
            heap.set(firstIndex, heap.get(secondIndex));
            heap.set(secondIndex, temp);
        }
    	
        /*
         * Heapify algorithm for the min heap.
         */
        private void minHeapify(int index) {
        	
          if(!isLeaf(index)) {
            int swapIndex = index;

            if(rightChild(index) <= size)
                swapIndex = heap.get(leftChild(index)).arrivalTime < heap.get(rightChild(index)).arrivalTime ? leftChild(index) : rightChild(index);
            else
                swapIndex = leftChild(index);
             
            if(heap.get(index).arrivalTime > heap.get(leftChild(index)).arrivalTime || heap.get(index).arrivalTime > heap.get(rightChild(index)).arrivalTime) {
              swap(index,swapIndex);
              minHeapify(swapIndex);
            }
          }
        }
        
        /*
         * Inserts a new node with the given 
         * element into the heap.
         */
        public void insert(Job element) {
     
            heap.add(++size, element);
            int currentIndex = size;
     
            while (heap.get(currentIndex).arrivalTime < heap.get(parent(currentIndex)).arrivalTime) {
                swap(currentIndex, parent(currentIndex));
                currentIndex = parent(currentIndex);
            }
        }
        
        /*
         * Removes and returns the minimum element
         * from the heap.
         */
        public Job remove() {
        	Job removed = heap.get(FRONT);
        	
        	if(size > 1) {
            	heap.set(FRONT, heap.get(size--));
            	heap.set(size+1, null);
        	}
        	
        	else if(size <= 1){
        		heap.set(FRONT, null);
        		size--;
        		return removed;
        	}

            minHeapify(FRONT);
            return removed;
        }
        
        /*
         * Returns the height of the heap.
         */
        public int getHeight(int index){
            if(index > size || heap.get(index) == null)
                return 0;
            return 1+Math.max(getHeight(index*2), getHeight(index*2+1));
        }
        
        /*
         * Fills the rows of answer.
         */
        public void setRows(int index, List<List<String>> answer, int i, int height, int left, int right){
            if(i >= height || heap.get(index) == null) 
            	return;
            int mid = (left+right)/2;
            answer.get(i).set(mid, ""+heap.get(index).jobId);
            setRows(index*2, answer, i+1, height, left, mid-1);
            setRows(index*2+1, answer, i+1, height, mid+1, right);
        }
        
        /*
         * Prints the heap in the tree format.
         */
        public List<List<String>> print(int index) {
            int height = getHeight(index);
            List<List<String>> answer = new ArrayList<>();
            int width = (int) Math.pow(2, height)-1;
            List<String> row = new ArrayList<>();
            for(int i=0;i<width;i++)
                row.add("");
            for(int i=0;i<height;i++)
                answer.add(new ArrayList<String>(row));
            setRows(index, answer, 0, height, 0, width-1);
            return answer;
        }
        
    }

    public MinHeap schedulerTree; // The min heap used to store read jobs.
    public Integer timer = 0; // Variable used as the system timer.
    public String filePath; // Variable used to store the path of Jobs.txt in a String.
    public HashMap<Integer, ArrayList<Integer>> dependencyMap; // Variable used to store dependency information, stores the dependencies in an Integer ArrayList.
    public ArrayList<Job> allJobs; // Variable used to store all of the read jobs.
    public Resource[] resources; // Variable used to store the resources.
    public int line; // Variable used to store the # of line to be read next.
    
    /*
     * Job class implementation
     */
	public class Job {
		
		public boolean isCompleted; // Stores the current situation of the job
		public int arrivalTime; // Stores arrival time of the job
		public Integer resource; // Stores resource information of the job
		public Integer jobId; // Stores id information of the job
		public int startTime; // Stores start time information of the job
		public int endTime;	// Stores end time information of the job
		public Integer delay; // Stores delay information of the job
		public int duration; // Stores the duration time of the job
		public boolean dependencyBlocked; // Stores the information of dependency block of the job
		public boolean resourceBlocked; // Stores the information of resource block of the job
		
		Job(Integer jobId, int arrivalTime, int duration) {
			this.dependencyBlocked = false;
			this.resourceBlocked = false;
			this.duration = duration;
			this.isCompleted = false;
			this.arrivalTime = arrivalTime;
			this.delay = null;
			this.jobId = jobId;
			this.startTime = 0;
			this.endTime = 0;
			this.resource = null;
		}
		
	    public boolean isDependent() {
	    	if(dependencyMap.get(this.jobId) != null) {
				ArrayList<Integer> dependencyList = dependencyMap.get(this.jobId);
				for(int i = 0; i < dependencyList.size(); i++) {
					if(allJobs.get(dependencyList.get(i)) != null) {
						if(!allJobs.get(dependencyList.get(i)).isCompleted) {
							return true;
						}
					}
					else if(allJobs.get(dependencyList.get(i)) == null) {
						return true;
					}
				}
			}
	    	
			boolean allResourcesBusy = true;
			for(int i = 0; i < resources.length; i++) {
				if(resources[i].isEmpty()) {
					allResourcesBusy = false;
					break;
				}
			}
			if(allResourcesBusy) {
				return true;
			}
	    	
	    	return false;
	    }
		
	}
	
	/*
	 * Resource class implementation
	 */
	public class Resource {
		
		private int id; // Stores id of the resource
		private boolean empty; // Stores the current situation of the resource
		private Job currentJob; // Stores the current job that is being worked on the resource
		private int lastjobstartTime; // Stores the start time of the last job of the resource
		private int lastjobendTime; // Stores the end time of the last job of the resource
		private int totalWork; // Stores the total work resource has made
		
		Resource(int id) {
			this.id = id;
			this.empty = true;
			this.currentJob = null;
			this.lastjobendTime = -1;
			this.lastjobstartTime = -1;
			this.totalWork = 0;
		}
		
		/*
		 * Returns true if the resource is empty,
		 * false otherwise.
		 */
		public boolean isEmpty() {
			return this.empty;
		}
		
		/*
		 * Puts the parameter job into the resource
		 * and starts the work process. Updates the
		 * current situation of the resource and related
		 * information of the given job.
		 */
		public void startWork(Job job) {
			this.empty = false;
			this.currentJob = job;
			this.currentJob.resourceBlocked = false;
			this.lastjobstartTime = timer;
			allJobs.get(this.currentJob.jobId).dependencyBlocked = false;
			allJobs.get(this.currentJob.jobId).resourceBlocked = false;
			allJobs.get(this.currentJob.jobId).startTime = this.lastjobstartTime;
			allJobs.get(this.currentJob.jobId).delay = timer - this.currentJob.arrivalTime;
			allJobs.get(this.currentJob.jobId).resource = this.id;
		}
		
		/*
		 * Stops the current work of the resource,
		 * updates the current situation of the resource
		 * and related information of the completed job.
		 */
		public void stopWork() {
			this.lastjobendTime = timer - 1;
			allJobs.get(this.currentJob.jobId).endTime = this.lastjobendTime;
			allJobs.get(this.currentJob.jobId).isCompleted = true;
			this.empty = true;
			this.totalWork = this.totalWork + ((this.lastjobendTime - this.lastjobstartTime) + 1);
			/*
			 * Deleting the done job from the dependencyMap.
			 */
			for(int i = 0; i < allJobs.size(); i++) {
				ArrayList<Integer> tempList = null;
				if(allJobs.get(i) != null && allJobs.get(i).jobId != this.currentJob.jobId) {
					tempList = dependencyMap.get(allJobs.get(i).jobId);
				}
				if(tempList != null) {
					for(int k = 0; k < tempList.size(); k++) {
						if(tempList.get(k) == this.currentJob.jobId) {
							tempList.remove(k);
						}
					}
				}
			}
			this.currentJob = null;
		}
		
	}

    
    /*
     * Constructor method of JobScheduler.
     * Gets file path of Jobs.txt as String and stores it in filePath,
     * also sets dependencyMap to an empty HashMap.
     */
    public JobScheduler(String filePath) {
        this.filePath = filePath;
        this.dependencyMap = new HashMap<Integer, ArrayList<Integer>>();
        this.schedulerTree = new MinHeap();
        this.allJobs = new ArrayList<Job>();
        this.line = 1;
    }

    /*
     * Gets a file path stored in a String as parameter 
     * and reads the dependency data of jobs from that file.
     * 
	 * Uses Scanner to read each line of the file word by word.
     * Stores the read jobIds as Integer variables named jobId1 and jobId2,
     * when both ids are read, it inserts the dependency to the dependencyMap.
     */
    public void insertDependencies(String dependencyPath){
    	
    	try {
    		Scanner scanner = new Scanner(new File(dependencyPath));
    		Integer jobId1 = null;
    		Integer jobId2 = null;
    		
    		while (scanner.hasNext()) {
    			
    			if(jobId1 == null) {
    				jobId1 = scanner.nextInt(); // Reading jobId1.
    				continue;
    			}
    			else if(jobId2 == null) {
    				/* Reading the dependency and adding it to the dependencyMap. */
    				jobId2 = scanner.nextInt();
    				ArrayList<Integer> temporaryList = dependencyMap.getOrDefault(jobId1, new ArrayList<Integer>());
    				temporaryList.add(jobId2);
    				temporaryList.trimToSize();
    				dependencyMap.put(jobId1, temporaryList);
    				
    			   	temporaryList = null; // Setting temporaryList to null to help garbage collector.
    			   	
    				/* Setting both jobIds to null to read new line from the file. */
    				jobId1 = null;
    				jobId2 = null;
    			}
    		}
    	
       	}
    	catch (IOException e) {
    		  System.out.println("Error accessing Dependencies.txt !");
    	}
    	
    }
    
    /*
     * Checks if there is a new line in the file
     * stored in filePath by using Scanner, returns true if 
     * there is a new line, false otherwise.
     */
    public boolean stillContinues(){
    	
    	try {
	        File jobsFile = new File(filePath);
	        BufferedReader br = new BufferedReader(new FileReader(jobsFile));
	        String tempLine = "";
	        int tempLineCounter = 0;
	        while((tempLine = br.readLine()) != null) {
	        	tempLineCounter++;
	        }
	        br.close();
	        if(this.line > tempLineCounter)
	        	return false;
	        
    	}
    	catch(IOException e) {
    		System.out.println("Error accessing Jobs.txt !");
    	}

        return true;
    }
    
    /*
     * Creates a temporary ArrayList called waitlist,
     * stops the work of resources that need to be stopped,
     * iterates over the schedulerTree until it finds a non
     * dependent job and puts dependent jobs into the waitlist 
     * while iterating, inserts the non dependent jobs into 
     * empty resources.
     */
    public void run(){
    	
    	
		// For loop to stop the resources that have finished their job
		for(int i = 0; i < resources.length; i++) {
			if(!resources[i].isEmpty() && timer == resources[i].lastjobstartTime + resources[i].currentJob.duration ) {
				resources[i].stopWork();
			}
		}
			
		/*
		 * Iterating the heap by removing front element
		 * at each iteration and pushing it to the waitlist
		 * if it is dependent until we find a job that is not
		 * dependent.
		 */
		if(schedulerTree.size != 0) {
			
			Job front = schedulerTree.remove();
			boolean allJobsDependent = false;
			ArrayList<Job> waitlist = new ArrayList<Job>(); // Variable used to store blocked jobs.
			
			
			while(front.isDependent()) {
				
				/*
				 * Assigning the dependency variables of the
				 * dependent job to an ArrayList called temp.
				 */
				ArrayList<Integer> temp = dependencyMap.get(front.jobId);
				
				if(temp != null) {
					for(int i = 0; i < temp.size(); i++) {
						if(temp.get(i) != null && allJobs.get(temp.get(i)) != null) {
							if(!allJobs.get(temp.get(i)).isCompleted) {
								front.dependencyBlocked = true;
							}
						}
						else if(temp.get(i) != null && allJobs.get(temp.get(i)) == null)
							front.dependencyBlocked = true;
					}
				}
				if(!front.dependencyBlocked)
					front.resourceBlocked = true;
				
				if(schedulerTree.size != 0) {
					waitlist.add(front);
					waitlist.trimToSize();
					front = schedulerTree.remove();
				}
				else {
					waitlist.add(front);
					waitlist.trimToSize();
					allJobsDependent = true;
					break;
				}
			}
			
			
			for(int i = 0; i < waitlist.size(); i++) {
				schedulerTree.insert(waitlist.get(i));
			}
			
			boolean recursiveCheck = false;
			if(!allJobsDependent) {
				for(int i = 0; i < resources.length; i++) {
					if(resources[i].isEmpty()) {
						resources[i].startWork(front);
						
						if(schedulerTree.size != 0)
							recursiveCheck = true;
						break;
					}
				}
			}
			
			if(recursiveCheck)
				run();
		}
    }

    /*
     * Sets the resource count, initializes
     * and fills the resources array.
     */
    public void setResourcesCount(Integer count){
    	resources = new Resource[count];
    	for(int i = 0; i < count; i++) {
    		resources[i] = new Resource(i+1);
    	}
    }

    /*
     * Reads a new line from the filePath, if
     * it reads a job inserts it into the schedulerTree
     * and allJobs, if it reads "no job" it does not do
     * anything. Deletes the line it read from the file.
     */
    public void insertJob(){
    	this.timer++;
    	
    	try {
	        File jobsFile = new File(filePath);
	        BufferedReader br = new BufferedReader(new FileReader(jobsFile));
	        String tempLine = "", firstLine = "";
	        int tempLineCounter = 1;
	        while((tempLine = br.readLine()) != null) {
	        	if(tempLineCounter == this.line) {
	        		firstLine = tempLine;
	        		this.line++;
	        		break;
	        	}
	        	tempLineCounter++;
	        }
	        br.close();
	        
	        if(firstLine.equals("no job")) {
	        	return;
	        }
	        else {
	        	String[] splitted = firstLine.split("\\s+");
	        	Integer jobId = Integer.parseInt(splitted[0]);
	        	Integer duration = Integer.parseInt(splitted[1]);
	        	
	        	Job currentJob = new Job(jobId, timer, duration);
	        	
	        	schedulerTree.insert(currentJob);
	        	
	        	if(jobId > allJobs.size() - 1) {
	        		for(int i = allJobs.size(); i < jobId; i++) {
	        			allJobs.add(null);
	        		}
	        		allJobs.add(jobId, currentJob);
	        	}
	        	else {
	        		allJobs.set(jobId, currentJob);
	        	}
	        	
	        	allJobs.trimToSize();

	        }
	        
	    }
    	catch (IOException e) {
    		System.out.println("Error accessing Jobs.txt !");
	    }
    	

    }

    /*
     * Prints completed jobs at the present time.
     */
    public void completedJobs(){
    	String text = "";
    	for(int i = 0; i < allJobs.size(); i++) {
    		if(allJobs.get(i) != null && allJobs.get(i).isCompleted) {
    			text = text + allJobs.get(i).jobId + ", ";
    		}
    	}
    	
    	if(text.length() >= 3)
    		text = text.substring(0, text.length()-2);
    	System.out.println("completed jobs " + text);
    }

    /*
     * Prints the dependency blocked jobs at
     * the present time.
     */
    public void dependencyBlockedJobs(){
    	String text = "dependency blocked jobs ";
    	
    	for(int i = 0; i < allJobs.size(); i++) {
    		if(allJobs.get(i) != null) {
	    		if(allJobs.get(i).dependencyBlocked) {
	    			ArrayList<Integer> tempList = dependencyMap.get(allJobs.get(i).jobId);
	    			String dependencies = "";
	    			for(int k = 0; k < tempList.size(); k++) {
	    				dependencies = dependencies + tempList.get(k) + "&";
	    			}
	    			dependencies = dependencies.substring(0, dependencies.length()-1);
	    			
	    			text = text + "(" + allJobs.get(i).jobId + "," + dependencies + ") ";
	    		}
    		}
    	}
    	System.out.println(text);
    	
    }

    /*
     * Prints the resource blocked jobs at
     * the present time.
     */
    public void resourceBlockedJobs(){
    	String text = "resource blocked jobs ";
    	
    	for(int i = 0; i < allJobs.size(); i++) {
    		if(allJobs.get(i) != null) {
    			if(allJobs.get(i).resourceBlocked) {
    				text = text + allJobs.get(i).jobId + " ";
    			}
    		}
    	}
    	System.out.println(text);
    }

    /*
     * Prints jobs working on the present
     * cycle and the resource they are
     * working on.
     */
    public void workingJobs(){
    	String text = "working jobs ";
    	
    	for(int i = 0; i < resources.length; i++) {
    		if(resources[i].currentJob != null)
    			text = text + "(" + resources[i].currentJob.jobId + "," + resources[i].id + ") "; 
    	}
    	System.out.println(text);
    }

    /*
     * Checks if there are any waiting/working
     * jobs left in schedulerTree or resources
     * and gets them done.
     */
    public void runAllRemaining(){
    	if(schedulerTree.size != 0) {
    		while(schedulerTree.size != 0) {
    			this.timer++;
    			run();
    		}
    	}
    	for(int i = 0; i < resources.length; i++) {
    		if(!resources[i].empty) {
    			while(!resources[i].empty) {
    				this.timer++;
    				run();
    			}
    		}
    	}
    }
    
    /*
     * Prints all the timeline in a tableau
     * form. The timeline inclused timestamp
     * rows, resource columns and jobs.
     */
    public void allTimeLine(){
    	System.out.println("alltimeline");
    	System.out.print("\t ");
    	for(int i = 0; i < resources.length; i++) {
    		System.out.print("R" + resources[i].id + "\t");
    	}
    	System.out.println();
    	for(int i = 1; i < this.timer; i++) {
    		System.out.print(i + "\t ");
    		for(int j = 0; j < resources.length; j++) {
    			boolean jobFound = false;
    			for(int k = 0; k < allJobs.size(); k++) {
    				if(allJobs.get(k) != null) {
		    			if(allJobs.get(k).startTime == i || ( allJobs.get(k).startTime < i && allJobs.get(k).endTime >= i )) {
		    				if(allJobs.get(k).resource == resources[j].id) {
		    					jobFound = true;
		    					System.out.print(allJobs.get(k).jobId + "\t");
		    					break;
		    				}
		    			}
    				}
	    		}
    			if(!jobFound) {
    				System.out.print("\t");
    			}
    			
    		}
    		System.out.println();
    		
    	}
    	
    }

    /*
     * Prints the schedulerTree in
     * tree format.
     */
    public String toString(){
    	String text = "";
    	
    	List<List<String>> answerList = schedulerTree.print(1);
    	for(int i = 0; i < answerList.size(); i++) {
    		List<String> row = answerList.get(i);
    		for(int k = 0; k < row.size(); k++) {
    			if(row.get(k).equals(""))
    				text = text + " ";
    			else
    				text = text + row.get(k);
    		}
    		text = text + "\n";
    	}
    	if(text.length()-1 > 0)
    		text = text.substring(0, text.length()-1);
    	
        return text;
    }
}