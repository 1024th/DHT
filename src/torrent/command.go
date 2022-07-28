package torrent

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/chzyer/readline"
	"github.com/sirupsen/logrus"
)

var self Client

// Function constructor - constructs new function for listing given directory
func listFiles(path string) func(string) []string {
	return func(line string) []string {
		names := make([]string, 0)
		files, _ := ioutil.ReadDir(path)
		for _, f := range files {
			names = append(names, f.Name())
		}
		return names
	}
}

var completer = readline.NewPrefixCompleter(
	readline.PcItem("create"),
	readline.PcItem("join"),
	readline.PcItem("upload",
		readline.PcItemDynamic(listFiles("./"),
			readline.PcItem("-t"),
		),
	),
	readline.PcItem("download",
		readline.PcItem("-t",
			readline.PcItemDynamic(listFiles("./")),
		),
		readline.PcItem("-m")),
	readline.PcItem("quit"),
	readline.PcItem("help"),
	readline.PcItem("exit"),
)

func filterInput(r rune) (rune, bool) {
	switch r {
	// block CtrlZ feature
	case readline.CharCtrlZ:
		return r, false
	}
	return r, true
}

func PrintHelp() {
	s := `BiteTorrent is a P2P file sharing software.
Commands:
  create                       Create a new network
  join [address]               Join the network containing the node in given IP address
  upload [file] -t [torrent]   Upload a file generate .torrent file
  download -t [torrent] [file] Download by .torrent file and save to given file path
  download -m [magnet] [file]  Download by magnet link and save to given file path
  quit                         Quit current network
  help                         Show help
  exit                         Exit the program
`
	yellow.Print(s)
}

func RunCLI() {
	blue.Println("+---------------+")
	blue.Println("|  BiteTorrent  |")
	blue.Println("+---------------+")

	blue.Print("Input your port number: ")
	var port int
	fmt.Scanln(&port)
	self.Init(port)

	blue.Println("Type \"help\" for more infomation, type \"exit\" to exit this program.")
	blue.Println("Press Tab for auto completion, press arrow up/down key to view command history.")
	logrus.SetOutput(os.Stderr)

	l, err := readline.NewEx(&readline.Config{
		Prompt:          "\033[31m»\033[0m ",
		HistoryFile:     "/tmp/readline.tmp",
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",

		HistorySearchFold:   true,
		FuncFilterInputRune: filterInput,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()
	l.CaptureExitSignal()

	setPasswordCfg := l.GenPasswordConfig()
	setPasswordCfg.SetListener(func(line []rune, pos int, key rune) (newLine []rune, newPos int, ok bool) {
		l.SetPrompt(fmt.Sprintf("Enter password(%v): ", len(line)))
		l.Refresh()
		return nil, 0, false
	})

	log.SetOutput(l.Stderr())
	for {
		line, err := l.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		args := strings.Split(line, " ")
		cmd := args[0]
		switch cmd {
		case "help":
			PrintHelp()
		case "create":
			self.Create()
		case "join":
			self.Join(args[1])
		case "upload":
			if len(args) < 4 {
				red.Println("Syntax error.")
				continue
			}
			self.Upload(args[1], args[3])
		case "download":
			if len(args) < 4 {
				red.Println("Syntax error.")
				continue
			}
			if args[1] == "-m" {
				self.DownLoadByMagnet(args[2], args[3])
			} else if args[1] == "-t" {
				self.DownLoadByTorrent(args[2], args[3])
			} else {
				red.Printf("Invalid download flag: %s.\n", args[1])
			}
		case "quit":
			self.Quit()
		case "exit":
			goto exit
		case "":
		default:
			fmt.Println("Unknown command:", line)
		}
	}
exit:
}
