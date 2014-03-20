package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"github.com/bmizerany/perks/quantile"
	"io"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	timeHandlers = 1  // threads pulling from the web, unzipping, chopping into lines (beware webserver throttle)
	lineHandlers = 8  // threads parsing lines into bson.M objects
	docHandlers  = 32 // threads pushing into mongo
)

func codeToProject(code string) (project string, err error) {
	switch code {
	case "b":
		project = "wikibooks"
	case "d":
		project = "wiktionary"
	case "m":
		project = "wikimedia"
	case "mw":
		project = "wikipedia mobile"
	case "n":
		project = "wikinews"
	case "q":
		project = "wikiquote"
	case "s":
		project = "wikisource"
	case "v":
		project = "wikiversity"
	case "w":
		project = "mediawiki"
	default:
		err = errors.New("unknown project")
	}
	return
}

type RawRecord struct {
	line string
	time time.Time
}

func parseLine(rec RawRecord) (entry bson.M, err error) {
	doc := make(bson.M)
	parts := strings.Split(rec.line, " ")
	siteparts := strings.Split(parts[0], ".")
	doc["lang"] = siteparts[0]
	doc["project"] = "wikipedia"
	if len(siteparts) > 1 {
		if doc["project"], err = codeToProject(siteparts[1]); err != nil {
			return
		}
	}
	unesc, err := url.QueryUnescape(parts[1])
	if err != nil {
		//log.Println("Error decoding", rec.line, ":", err)
		doc["url"] = parts[1]
		doc["malformed"] = true
	} else {
		doc["url"] = unesc
	}
	doc["requests"], err = strconv.Atoi(parts[2])
	if err != nil {
		return
	}
	doc["size"], err = strconv.Atoi(parts[3])
	if err != nil {
		return
	}
	doc["time"] = rec.time
	entry = doc
	return
}

func retryInsert(coll *mgo.Collection, batch ...interface{}) (err error) {
	for err := coll.Insert(batch...); err != nil; err = coll.Insert(batch...) {
		if lerr, ok := err.(*mgo.LastError); !ok || lerr.Code != 16759 {
			break
		}
		time.Sleep(time.Millisecond)
		log.Println("Retrying insert due to lock not granted")
	}
	return
}

func readFileIntoLines(reader io.Reader, date time.Time, lines chan<- RawRecord) (err error) {
	raw, err := gzip.NewReader(reader)
	if err != nil {
		return
	}
	defer raw.Close()

	sc := bufio.NewScanner(raw)
	for sc.Scan() {
		lines <- RawRecord{sc.Text(), date}
	}
	err = sc.Err()
	return
}

func main() {
	var (
		host     = flag.String("host", "localhost", "host:port string of database to connect to")
		dbname   = flag.String("db", "wiki", "dbname")
		collname = flag.String("coll", "rawlogs", "collname")
		source   = flag.String("source", "/mnt/wiki/wikistats/pagecounts/", "directory or http resource (e.g. http://dumps.wikimedia.org/other/pagecounts-raw/) containing pagecounts data")
	)

	flag.Parse()

	sess, err := mgo.Dial(*host)
	if err != nil {
		log.Fatal("Error connecting to ", *host, ": ", err)
	}
	defer sess.Close()
	//sess.SetSafe(nil) //&mgo.Safe{WMode: "majority"})
	coll := sess.DB(*dbname).C(*collname)
	if err := coll.DropCollection(); err != nil {
		log.Println(err)
	}

	var (
		linesDone            = sync.WaitGroup{}
		lines                = make(chan RawRecord, 1000)
		docsDone             = sync.WaitGroup{}
		docs                 = make(chan bson.M, 5000)
		insertCounter uint64 = 0
		kill                 = make(chan bool)
		cq                   = quantile.NewTargeted(0.50, 0.90, 0.99)
		wq                   = quantile.NewTargeted(0.50, 0.90, 0.99)
		latencies            = make(chan time.Duration)
	)
	go func() {
		for t := range latencies {
			var v float64 = float64(t) / float64(time.Millisecond)
			cq.Insert(v)
			wq.Insert(v)
		}
	}()
	go func() {
		start := time.Now()
		ticks := time.NewTicker(time.Second)
		_, ok := <-ticks.C
		last := start
		var lastCount uint64 = 0
		var linesPrinted int = 0
		for ok {
			thisCount := atomic.LoadUint64(&insertCounter)
			diff := thisCount - lastCount
			now := time.Now()
			idur := now.Sub(last)
			cdur := now.Sub(start)
			if linesPrinted%24 == 0 {
				log.Printf("-- docs -- -------- ips ------- ------- window latency ------- ----- cumulative latency ----\n")
				log.Printf("    total |     iter       cum |      50%%       90%%       99%% |      50%%       90%%       99%%\n")
			}
			linesPrinted++
			log.Printf("%9d |%9.2f %9.2f |%9.3f %9.3f %9.3f |%9.3f %9.3f %9.3f\n", thisCount, float64(diff)/idur.Seconds(), float64(thisCount)/cdur.Seconds(), wq.Query(0.50), wq.Query(0.90), wq.Query(0.99), cq.Query(0.50), cq.Query(0.90), cq.Query(0.99))
			wq.Reset()
			select {
			case _, ok = <-ticks.C:
				lastCount = thisCount
				last = now
			case <-kill:
				break
			}
		}
	}()
	for i := 0; i < lineHandlers; i++ {
		linesDone.Add(1)
		go func() {
			for rec := range lines {
				doc, err := parseLine(rec)
				if err != nil {
					log.Fatal("Error parsing ", rec.line, ": ", err)
				} else {
					docs <- doc
				}
			}

			linesDone.Done()
		}()
	}
	for i := 0; i < docHandlers; i++ {
		docsDone.Add(1)
		go func() {
			workerSession := sess.New()
			defer workerSession.Close()
			coll := workerSession.DB("wiki").C("rawlogs")
			batch := make([]interface{}, 0, 100)
			for doc := range docs {
				batch = append(batch, doc)
				if len(batch) == cap(batch) {
					t0 := time.Now()
					if err := retryInsert(coll, batch...); err != nil {
						log.Fatal("Error inserting batch: ", err)
					}
					atomic.AddUint64(&insertCounter, uint64(len(batch)))
					latencies <- time.Since(t0)
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				t0 := time.Now()
				if err := retryInsert(coll, batch...); err != nil {
					log.Fatal("Error inserting batch: ", err)
				}
				atomic.AddUint64(&insertCounter, uint64(len(batch)))
				latencies <- time.Since(t0)
			}
			docsDone.Done()
		}()
	}

	if strings.HasPrefix(*source, "http://") {
		var (
			timesDone = sync.WaitGroup{}
			times     = make(chan time.Time, 10)
		)
		for i := 0; i < timeHandlers; i++ {
			timesDone.Add(1)
			go func() {
				for t := range times {
					filename := fmt.Sprintf(*source+"%04d/%04d-%02d/pagecounts-%04d%02d%02d-%02d0000.gz", t.Year(), t.Year(), t.Month(), t.Year(), t.Month(), t.Day(), t.Hour())
					resp, err := http.Get(filename)
					if err != nil {
						if err == io.EOF || strings.HasSuffix(err.Error(), "EOF") {
							log.Println("EOF getting file ", filename)
							times <- t
							time.Sleep(time.Second)
							continue
						}
						log.Fatal("Error getting file ", filename, ": ", err)
					}
					if resp.StatusCode == 404 {
						resp.Body.Close()
						filename = fmt.Sprintf(*source+"%04d/%04d-%02d/pagecounts-%04d%02d%02d-%02d0001.gz", t.Year(), t.Year(), t.Month(), t.Year(), t.Month(), t.Day(), t.Hour())
						resp, err = http.Get(filename)
						if err != nil {
							if err == io.EOF || strings.HasSuffix(err.Error(), "EOF") {
								log.Println("EOF getting file ", filename)
								times <- t
								time.Sleep(time.Second)
								continue
							}
							log.Fatal("Error getting file ", filename, ": ", err)
						}
					}
					defer resp.Body.Close()

					if resp.StatusCode != 200 {
						log.Println("HTTP status", resp.StatusCode, "while getting", filename)
						continue
					}

					if err := readFileIntoLines(resp.Body, t, lines); err != nil {
						log.Fatal(err)
					}
				}

				timesDone.Done()
			}()
		}

		var twoThousandFourteen = time.Date(2014, time.January, 1, 0, 0, 0, 0, time.UTC)
		for date := time.Date(2008, time.January, 1, 0, 0, 0, 0, time.UTC); date.Before(twoThousandFourteen); date = date.Add(time.Hour) {
			times <- date
		}

		close(times)
		timesDone.Wait()
	} else {
		texasRanger := func(path string, info os.FileInfo, ein error) (err error) {
			if info.IsDir() {
				return
			}

			file, err := os.Open(path)
			if err != nil {
				log.Fatal("Error opening ", path, ": ", err)
			}
			defer file.Close()

			re := regexp.MustCompile("pagecounts-(\\d{4})(\\d{2})(\\d{2})-(\\d{2})000[01].gz")
			matches := re.FindStringSubmatch(filepath.Base(path))
			if len(matches) == 0 {
				log.Fatal("Unexpected filename ", path)
			}
			year, err := strconv.Atoi(matches[1])
			if err != nil {
				return
			}
			month, err := strconv.Atoi(matches[2])
			if err != nil {
				return
			}
			day, err := strconv.Atoi(matches[3])
			if err != nil {
				return
			}
			hour, err := strconv.Atoi(matches[4])
			if err != nil {
				return
			}

			date := time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC)

			err = readFileIntoLines(file, date, lines)
			return
		}

		filepath.Walk(*source, texasRanger)
	}

	close(lines)
	linesDone.Wait()
	close(docs)
	docsDone.Wait()
	close(latencies)
	kill <- true
}
