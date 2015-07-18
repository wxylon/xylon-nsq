// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"log"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"
)

var (
	datetimeFormat   = flag.String("datetime-format", "%Y-%m-%d_%H", "strftime compatible format for <DATETIME> in filename format")
	filenameFormat   = flag.String("filename-format", "<TOPIC>.<HOST><GZIPREV>.<DATETIME>.log", "output filename format (<TOPIC>, <HOST>, <DATETIME>, <GZIPREV> are replaced. <GZIPREV> is a suffix when an existing gzip file already exists)")
	showVersion      = flag.Bool("version", false, "print version string")
	hostIdentifier   = flag.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	outputDir        = flag.String("output-dir", "/tmp", "directory to write output files to")
	topic            = flag.String("topic", "", "nsq topic")
	channel          = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight      = flag.Int("max-in-flight", 1000, "max number of messages to allow in flight")
	gzipCompression  = flag.Int("gzip-compression", 3, "gzip compression level. 1 BestSpeed, 2 BestCompression, 3 DefaultCompression")
	gzipEnabled      = flag.Bool("gzip", false, "gzip output files.")
	verbose          = flag.Bool("verbose", false, "verbose logging")
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type FileLogger struct {
	out              *os.File
	gzipWriter       *gzip.Writer
	lastFilename     string
	logChan          chan *Message
	compressionLevel int
	gzipEnabled      bool
	filenameFormat   string
}

type Message struct {
	*nsq.Message
	returnChannel chan *nsq.FinishedMessage
}

type SyncMsg struct {
	m             *nsq.FinishedMessage
	returnChannel chan *nsq.FinishedMessage
}

func (l *FileLogger) HandleMessage(m *nsq.Message, responseChannel chan *nsq.FinishedMessage) {
	l.logChan <- &Message{m, responseChannel}
}

func router(r *nsq.Reader, f *FileLogger, termChan chan os.Signal, hupChan chan os.Signal) {
	pos := 0
	output := make([]*Message, *maxInFlight)
	sync := false
	ticker := time.NewTicker(time.Duration(30) * time.Second)
	closing := false

	for {
		select {
		case <-termChan:
			ticker.Stop()
			r.Stop()
			// ensures that we keep flushing whatever is left in the channels
			closing = true
		case <-hupChan:
			f.Close()
			f.updateFile()
			sync = true
		case <-ticker.C:
			f.updateFile()
			sync = true
		case m := <-f.logChan:
			if f.updateFile() {
				sync = true
			}
			_, err := f.Write(m.Body)
			if err != nil {
				log.Fatalf("ERROR: writing message to disk - %s", err.Error())
			}
			_, err = f.Write([]byte("\n"))
			if err != nil {
				log.Fatalf("ERROR: writing newline to disk - %s", err.Error())
			}
			output[pos] = m
			pos++
			if pos == *maxInFlight {
				sync = true
			}
		}

		if closing || sync || r.IsStarved() {
			if pos > 0 {
				log.Printf("syncing %d records to disk", pos)
				err := f.Sync()
				if err != nil {
					log.Fatalf("ERROR: failed syncing messages - %s", err.Error())
				}
				for pos > 0 {
					pos--
					m := output[pos]
					m.returnChannel <- &nsq.FinishedMessage{m.Id, 0, true}
					output[pos] = nil
				}
			}
			sync = false
		}
	}
}

func (f *FileLogger) Close() {
	if f.out != nil {
		if f.gzipWriter != nil {
			f.gzipWriter.Close()
		}
		f.out.Close()
		f.out = nil
	}
}
func (f *FileLogger) Write(p []byte) (n int, err error) {
	if f.gzipWriter != nil {
		return f.gzipWriter.Write(p)
	}
	return f.out.Write(p)
}
func (f *FileLogger) Sync() error {
	var err error
	if f.gzipWriter != nil {
		f.gzipWriter.Close()
		err = f.out.Sync()
		f.gzipWriter, _ = gzip.NewWriterLevel(f.out, f.compressionLevel)
	} else {
		err = f.out.Sync()
	}
	return err
}

func (f *FileLogger) calculateCurrentFilename() string {
	t := time.Now()

	datetime := strftime(*datetimeFormat, t)
	filename := strings.Replace(f.filenameFormat, "<DATETIME>", datetime, -1)
	if !f.gzipEnabled {
		filename = strings.Replace(filename, "<GZIPREV>", "", -1)
	}
	return filename

}

func (f *FileLogger) updateFile() bool {
	filename := f.calculateCurrentFilename()
	maxGzipRevisions := 1000
	if filename != f.lastFilename || f.out == nil {
		f.Close()
		os.MkdirAll(*outputDir, 777)
		var newFile *os.File
		var err error
		if f.gzipEnabled {
			// for gzip files, we never append to an existing file
			// we try to create different revisions, replacing <GZIPREV> in the filename
			for gzipRevision := 0; gzipRevision < maxGzipRevisions; gzipRevision += 1 {
				var revisionSuffix string
				if gzipRevision > 0 {
					revisionSuffix = fmt.Sprintf("-%d", gzipRevision)
				}
				tempFilename := strings.Replace(filename, "<GZIPREV>", revisionSuffix, -1)
				fullPath := path.Join(*outputDir, tempFilename)
				newFile, err = os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
				if err != nil && os.IsExist(err) {
					log.Printf("INFO: file already exists: %s", fullPath)
					continue
				}
				if err != nil {
					log.Fatalf("ERROR: %s Unable to open %s", err, fullPath)
				}
				log.Printf("opening %s", fullPath)
				break
			}
			if newFile == nil {
				log.Fatalf("ERROR: Unable to open a new gzip file after %d tries", maxGzipRevisions)
			}
		} else {
			log.Printf("opening %s/%s", *outputDir, filename)
			newFile, err = os.OpenFile(path.Join(*outputDir, filename), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				log.Fatal(err)
			}
		}

		f.out = newFile
		f.lastFilename = filename
		if f.gzipEnabled {
			f.gzipWriter, _ = gzip.NewWriterLevel(newFile, f.compressionLevel)
		}
		return true
	}

	return false
}

func NewFileLogger(gzipEnabled bool, compressionLevel int, filenameFormat string) (*FileLogger, error) {
	var speed int
	switch compressionLevel {
	case 1:
		speed = gzip.BestSpeed
	case 2:
		speed = gzip.BestCompression
	case 3:
		speed = gzip.DefaultCompression
	}

	if gzipEnabled && strings.Index(filenameFormat, "<GZIPREV>") == -1 {
		return nil, errors.New("missing <GZIPREV> in filenameFormat")
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	shortHostname := strings.Split(hostname, ".")[0]
	identifier := shortHostname
	if len(*hostIdentifier) != 0 {
		identifier = strings.Replace(*hostIdentifier, "<SHORT_HOST>", shortHostname, -1)
		identifier = strings.Replace(identifier, "<HOSTNAME>", hostname, -1)
	}
	filenameFormat = strings.Replace(filenameFormat, "<TOPIC>", *topic, -1)
	filenameFormat = strings.Replace(filenameFormat, "<HOST>", identifier, -1)
	if gzipEnabled && !strings.HasSuffix(filenameFormat, ".gz") {
		filenameFormat = filenameFormat + ".gz"
	}

	f := &FileLogger{
		logChan:          make(chan *Message, 1),
		compressionLevel: speed,
		filenameFormat:   filenameFormat,
		gzipEnabled:      gzipEnabled,
	}
	return f, nil
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_file v%s\n", util.BINARY_VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *maxInFlight < 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if *gzipCompression < 1 || *gzipCompression > 3 {
		log.Fatalf("invalid --gzip-compresion value (%v). should be 1,2 or 3", *gzipCompression)
	}

	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	f, err := NewFileLogger(*gzipEnabled, *gzipCompression, *filenameFormat)
	if err != nil {
		log.Fatal(err.Error())
	}

	r, err := nsq.NewReader(*topic, *channel)
	if err != nil {
		log.Fatalf(err.Error())
	}
	r.SetMaxInFlight(*maxInFlight)
	r.VerboseLogging = *verbose

	r.AddAsyncHandler(f)
	go router(r, f, termChan, hupChan)

	for _, addrString := range nsqdTCPAddrs {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdHTTPAddrs {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	<-r.ExitChan
}
