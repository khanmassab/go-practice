package main 

import (
	"fmt"
	"strings"
	"errors"
	"sync"
)

type WordCountResult struct {
	WordID int 
	WordCount int 
	Err error
}

type Job struct {
	JobID int
	JobString string
}

func countWords(doc string) (int, error) {
	if len(doc) <= 0 {
		return 0, errors.New("No words found in this document")
	}

	words := strings.Fields(doc)
	return len(words), nil 
}

func worker(id int, jobs <-chan Job, result chan<- WordCountResult, wg *sync.WaitGroup){
	defer wg.Done()

	for doc := range jobs {
		count, err := countWords(doc.JobString)

		if err != nil {
			result <- WordCountResult {
				WordID:  doc.JobID,
				Err: err,
			}

			continue
		}

		result <- WordCountResult {
			WordID: doc.JobID, 
			WordCount: count,
		}
	}
}

func main() {
	numWorkers := 2

	documents := []string{ 
			"The quick brown fox jumps over the lazy dog.",
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
			"To be, or not to be, that is the question.",
			"In the beginning God created the heavens and the earth.",
			"It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness.",
			"Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.",
			"The Great Gatsby by F. Scott Fitzgerald. The novel was first published in 1925.",
			"Data structures and algorithms are the backbone of computer science.",
			"She sells seashells by the seashore.",
			"Welcome to the word counting exercise! Testing string manipulation functions is a great way to learn programming logic.",
	}	

	jobs := make(chan Job, len(documents))
	result := make(chan WordCountResult) 

	var wg sync.WaitGroup

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, result, &wg)
	}

	for i:=1; i <= len(documents); i++ {
		jobs <- Job{JobID: i, JobString: documents[i-1]} 
	}
	close(jobs)

	go func() {
		wg.Wait()	
		close(result)
	}()

	for r := range result {
		if r.Err != nil {
			fmt.Printf("Document ID: %d;\t Error: %d \n", r.WordID, r.Err)
		}
		fmt.Printf("Document ID: %d;\t Word Count: %d \n", r.WordID, r.WordCount)
	}
}	
