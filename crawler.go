package main

import (
	"fmt"
	"time"
	"sync"
	"io"
	"net/http"
	"strings"
	"errors"
)

type FetchResult struct{
	URL string
	Latency time.Duration
	StatusCode int
	Body string
	Err error
}

type ParseResult struct{
	URL string
	LinkCount int
	Err error
}

func main () {
 	var wg sync.WaitGroup
 	var wg2 sync.WaitGroup

	fetchWorkers := 3 
	parseWorkers := 2

	urls := []string {
		"https://crawler-test.com",
		"https://httpbin.org",
		"https://httpstat.us",
		"https://toscrape.com",
		"https://toscrape.com",
		"https://google.com",
		"https://httpbin.org",
		"https://robotstxt.org",
		"http://example.com",
		"https://httpbin.org",
	}

	lenUrls := len(urls)
	urlChan := make(chan string, lenUrls)

	fetchOutput := make(chan FetchResult, lenUrls)
	parseOutput := make(chan ParseResult, lenUrls)

	for w := 1; w <= fetchWorkers; w++ {
		wg.Add(1)
		go fetchWorker(w, urlChan, fetchOutput, &wg)
	}

	for _, url := range urls {
		urlChan <- url
	}
	close(urlChan)

	go func(){
		wg.Wait()
		close(fetchOutput)
	}()
	
	for w := 1; w <= parseWorkers; w++ {
		wg2.Add(1)
		go parseWorker(w, fetchOutput, parseOutput, &wg2)
	}

	for res := range parseOutput {
		fmt.Println(res)
	}

	go func(){
		wg2.Wait()
		close(parseOutput)
	}()
}

func fetchWorker(id int, urls <-chan string, fetchOutput chan<- FetchResult, wg *sync.WaitGroup){
	defer wg.Done()
	for url := range urls {

		start := time.Now()
		resp, err := http.Get(url)
		latency := time.Since(start)

		if err == nil {
			partialBody := io.LimitReader(resp.Body, 50)
			body,_ := io.ReadAll(partialBody)

			fetchOutput <- FetchResult {
				URL: url,
				StatusCode: resp.StatusCode,
				Latency: latency, 
				Body: string(body),
			}

			continue
	}

		fetchOutput <- FetchResult {
			URL: url,
			Err: err,
		}
	}
}

func parseWorker(id int, fetchOutput <-chan FetchResult, parseOutput chan<- ParseResult, wg *sync.WaitGroup){
	defer wg.Done()

	for output := range fetchOutput { 
		linkCount := strings.Count(output.Body, "<a href=")

		if output.Err != nil {
			parseOutput <- ParseResult {
				URL: output.URL,
				Err: errors.New("There was an error fetching the URL so cannot process."),
			}
			continue
		}

		parseOutput <- ParseResult {
			URL: output.URL,
			LinkCount: linkCount,
		}
	}

}
