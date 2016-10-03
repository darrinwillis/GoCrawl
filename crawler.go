package main

import (
	"errors"
	"flag"
	"fmt"
	"golang.org/x/net/html"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime/pprof"
	"time"
)

import _ "net/http/pprof"

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func fetchUrls(urlsIn <-chan string, urlsOut chan<- string, fetcher Fetcher) {
	go func() {
		defer close(urlsOut)
		for urlIn := range urlsIn {
			_, urls, err := fetcher.Fetch(urlIn)
			if err != nil {
				// fmt.Println(err)
				continue
			}
			for _, u := range urls {
				urlsOut <- u
			}
		}
	}()
	return
}

func filterUrls(urlsIn <-chan string) <-chan string {
	filteredUrls := make(chan string)
	seenUrls := make(map[string]bool)
	// XXX - make this a bloom filter or something
	go func() {
		defer close(filteredUrls)
		for url := range urlsIn {
			_, seen := seenUrls[url]
			if !seen {
				seenUrls[url] = true
				filteredUrls <- url
			}
		}
	}()
	return filteredUrls
}

func loopBack(numUrls int, urlsIn <-chan string, urlsOut chan<- string) []string {
	var resultUrls []string
	for ii := 0; ii < numUrls; ii++ {
		url, ok := <-urlsIn
		if !ok {
			break
		}
		resultUrls = append(resultUrls, url)
		if ii%1000 == 0 {
			fmt.Printf("%d urls processed\n", ii)
		}
		urlsOut <- url
	}
	return resultUrls
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	seedUrl := "http://golang.org/"
	numToFind := 10000
	fetcher := HttpFetcher{}
	// this size should be the maximum amount of single-url fanout.
	needProcessed := make(chan string, 1000000)
	needProcessed <- seedUrl

	go func() {
		for range time.Tick(5 * time.Second) {
			l := len(needProcessed)
			c := cap(needProcessed)
			fmt.Printf("process queue length: %d of %d (%2.2f%%)\n", l, c, 100.0*float32(l)/float32(c))
		}
	}()

	// Make fetchers
	numFetchers := 500
	rawFound := make(chan string)
	for ii := 0; ii < numFetchers; ii++ {
		go fetchUrls(needProcessed, rawFound, fetcher)
	}
	filteredUrls := filterUrls(rawFound)
	finalUrls := loopBack(numToFind, filteredUrls, needProcessed)
	fmt.Printf("found %d urls", len(finalUrls))
	//for _, url := range finalUrls {
	//	fmt.Printf("Result url %s\n", url)
	//}
}

type HttpFetcher struct{}

func (f HttpFetcher) Fetch(inUrl string) (string, []string, error) {
	doc, err := getDocTree(inUrl)
	if err != nil {
		return "", nil, err
	}

	foundHrefs, err := getReferences(doc)
	if err != nil {
		return "", nil, err
	}

	foundUrls := makeUrls(foundHrefs, inUrl)

	//fmt.Printf("found %d branches from %s\n", len(foundUrls), inUrl)

	return inUrl, foundUrls, nil
}

func makeUrls(hrefs []string, inUrl string) []string {
	inParsed, _ := url.Parse(inUrl)

	// Get urls from hrefs
	foundUrls := make([]string, len(hrefs))
	for _, hr := range hrefs {
		parsed, err := url.Parse(hr)
		if err != nil {
			fmt.Println("url parse err", err)
			continue
		}
		// Fixup parsed url
		if parsed.Scheme == "" {
			parsed.Scheme = inParsed.Scheme
		}
		switch parsed.Scheme {
		case "irc":
			fallthrough
		case "aim":
			fallthrough
		case "mailto":
			fallthrough
		case "javascript":
			continue
		}
		if parsed.Host == "" {
			parsed.Host = inParsed.Host
		}
		parsed.ForceQuery = false
		parsed.RawQuery = ""
		parsed.Fragment = ""
		if str := parsed.String(); str[len(str)-1] == ':' {
			fmt.Printf("Url %s has fields:\n\n%#v\n\n", str, parsed)
		}
		foundUrls = append(foundUrls, parsed.String())
	}
	return foundUrls
}

func getReferences(doc *html.Node) ([]string, error) {
	// Get HREF tags from html tree
	foundHrefs := make([]string, 1)
	var processFunc func(*html.Node)
	processFunc = func(node *html.Node) {
		if node.Type == html.ElementNode {
			for _, attr := range node.Attr {
				if attr.Key == "href" {
					foundHrefs = append(foundHrefs, attr.Val)
				}
			}
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			processFunc(c)
		}
	}
	processFunc(doc)
	return foundHrefs, nil
}

func getDocTree(inUrl string) (*html.Node, error) {
	// ~1 MB
	var maxContentLength int64
	maxContentLength = 1000000

	resp, err := http.Get(inUrl)
	if err != nil {
		fmt.Println("http err", err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.ContentLength > maxContentLength {
		return nil, fmt.Errorf("file too large (%d bytes)", resp.ContentLength)
	}
	if resp.ContentLength < 0 {
		return nil, errors.New("file size unknown")
	}
	doc, err := html.Parse(resp.Body)
	return doc, err
}

func printUrl(u *url.URL) string {
	return fmt.Sprintf("scheme:%15v host:%15v Path:%15v", u.Scheme, u.Host, u.Path)
}
