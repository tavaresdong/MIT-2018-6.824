package mapreduce

import "fmt"

func worker(workerAddr string, jobs <-chan *DoTaskArgs, results chan<- int) {
	// worker will accept jobs
	// and then process them one by one
	for j := range jobs {
		ok := call(workerAddr, "Worker.DoTask", j, new(struct{}))
		if ok == false {
			fmt.Printf("Run Task failed on %s\n", workerAddr)
		} else {
			results <- 1
		}
	}
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	jobs := make(chan *DoTaskArgs, 100)
	results := make(chan int, 100)

	go func(rc chan string) {
		for {
			wk := <-rc
			go worker(wk, jobs, results)
		}
	}(registerChan)

	for i := 0; i < ntasks; i++ {
		args := new(DoTaskArgs)
		args.File = mapFiles[i]
		args.JobName = jobName
		args.NumOtherPhase = n_other
		args.Phase = phase
		args.TaskNumber = i

		jobs <- args
	}
	close(jobs)

	for i := 0; i < ntasks; i++ {
		<-results
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
