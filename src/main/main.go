package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	help     bool
	testName string
)

func init() {
	flag.BoolVar(&help, "help", false, "help")
	flag.StringVar(&testName, "test", "", "which test(s) do you want to run: basic/advance/all")

	flag.Usage = usage
	flag.Parse()

	if help || (testName != "basic" && testName != "advance" && testName != "all" && testName != "mytest") {
		flag.Usage()
		os.Exit(0)
	}

	seed := time.Now().UnixNano()
	fmt.Printf("Seed: %v\n", seed)
	rand.Seed(seed)
}

type myFormatter struct {
	logrus.TextFormatter
}

func (f *myFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// this whole mess of dealing with ansi color codes is required if you want the colored output otherwise you will lose colors in the log levels
	return []byte(fmt.Sprintf("%s[%s] %s", strings.ToUpper(entry.Level.String()), entry.Time.Format(f.TimestampFormat), entry.Message)), nil
}

func main() {
	f, _ := os.Create("/tmp/test.log")
	logrus.SetOutput(f)
	logrus.SetFormatter(&myFormatter{logrus.TextFormatter{
		FullTimestamp:          true,
		TimestampFormat:        "15:04:05.000000",
		DisableLevelTruncation: true,
	}})
	_, _ = yellow.Println("Welcome to DHT-2022 Test Program!\n")

	var basicFailRate float64
	var forceQuitFailRate float64
	var QASFailRate float64

	switch testName {
	// case "mytest":
	// 	mytest()
	case "all":
		fallthrough
	case "basic":
		_, _ = yellow.Println("Basic Test Begins:")
		basicPanicked, basicFailedCnt, basicTotalCnt := basicTest()
		if basicPanicked {
			_, _ = red.Printf("Basic Test Panicked.")
			os.Exit(0)
		}

		basicFailRate = float64(basicFailedCnt) / float64(basicTotalCnt)
		if basicFailRate > basicTestMaxFailRate {
			_, _ = red.Printf("Basic test failed with fail rate %.4f\n", basicFailRate)
		} else {
			_, _ = green.Printf("Basic test passed with fail rate %.4f\n", basicFailRate)
		}

		if testName == "basic" {
			break
		}
		time.Sleep(afterTestSleepTime)
		fallthrough
	case "advance":
		_, _ = yellow.Println("Advance Test Begins:")

		/* ------ Force Quit Test Begins ------ */
		forceQuitPanicked, forceQuitFailedCnt, forceQuitTotalCnt := forceQuitTest()
		if forceQuitPanicked {
			_, _ = red.Printf("Force Quit Test Panicked.")
			os.Exit(0)
		}

		forceQuitFailRate = float64(forceQuitFailedCnt) / float64(forceQuitTotalCnt)
		if forceQuitFailRate > forceQuitMaxFailRate {
			_, _ = red.Printf("Force quit test failed with fail rate %.4f\n", forceQuitFailRate)
		} else {
			_, _ = green.Printf("Force quit test passed with fail rate %.4f\n", forceQuitFailRate)
		}
		time.Sleep(afterTestSleepTime)
		/* ------ Force Quit Test Ends ------ */

		/* ------ Quit & Stabilize Test Begins ------ */
		QASPanicked, QASFailedCnt, QASTotalCnt := quitAndStabilizeTest()
		if QASPanicked {
			_, _ = red.Printf("Quit & Stabilize Test Panicked.")
			os.Exit(0)
		}

		QASFailRate = float64(QASFailedCnt) / float64(QASTotalCnt)
		if QASFailRate > QASMaxFailRate {
			_, _ = red.Printf("Quit & Stabilize test failed with fail rate %.4f\n", QASFailRate)
		} else {
			_, _ = green.Printf("Quit & Stabilize test passed with fail rate %.4f\n", QASFailRate)
		}
		/* ------ Quit & Stabilize Test Ends ------ */
	}

	_, _ = cyan.Println("\nFinal print:")
	if basicFailRate > basicTestMaxFailRate {
		_, _ = red.Printf("Basic test failed with fail rate %.4f\n", basicFailRate)
	} else {
		_, _ = green.Printf("Basic test passed with fail rate %.4f\n", basicFailRate)
	}
	if forceQuitFailRate > forceQuitMaxFailRate {
		_, _ = red.Printf("Force quit test failed with fail rate %.4f\n", forceQuitFailRate)
	} else {
		_, _ = green.Printf("Force quit test passed with fail rate %.4f\n", forceQuitFailRate)
	}
	if QASFailRate > QASMaxFailRate {
		_, _ = red.Printf("Quit & Stabilize test failed with fail rate %.4f\n", QASFailRate)
	} else {
		_, _ = green.Printf("Quit & Stabilize test passed with fail rate %.4f\n", QASFailRate)
	}
}

func usage() {
	flag.PrintDefaults()
}
