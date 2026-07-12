package main 

import "fmt"
import "net/http"
import "sync"

type CrawlResult struct {
	URL string
	StatusCode int
	Status string
	Err error
}

func main () {
	var wg sync.WaitGroup

	goCrawlUrls := []string {
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

	respChan := make(chan CrawlResult)
	fmt.Println("Execution Started..")

	for _, url := range goCrawlUrls{ 
		wg.Add(1)
		go hitUrl(url, respChan, &wg)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	for msg := range respChan {

		if msg.Err != nil {
			fmt.Printf("%s -> %v\n", msg.URL, msg.Err)
		}

		fmt.Printf("%s -> %s %d\n", msg.URL, msg.Status, msg.StatusCode)
	}

	print("Execution Finished..")
}

func hitUrl(url string, respChan chan CrawlResult, wg *sync.WaitGroup) {
	defer wg.Done()

	resp, err := http.Get(url)

	if err != nil {
			respChan <- CrawlResult {
				URL: url,
				Err: err,
			} 

			return
	}
	
	defer resp.Body.Close()
	respChan <- CrawlResult {
		URL: url,
		StatusCode: resp.StatusCode,
		Status: resp.Status,
	}
}
