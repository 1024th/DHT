package torrent

import (
	"kademlia"
	"net"
	"strconv"
	"time"

	"github.com/fatih/color"
)

var (
	green  = color.New(color.FgGreen)
	red    = color.New(color.FgRed)
	yellow = color.New(color.FgYellow)
	cyan   = color.New(color.FgCyan)
	blue   = color.New(color.FgBlue)
)

const (
	SHA1Len = 20

	PieceSize       = 1048576 // 1MB
	WorkQueueBuffer = 1024

	UploadTimeout    = time.Second
	UploadRetryNum   = 2
	DownloadTimeout  = time.Second
	DownloadRetryNum = 2
)

func MakeMagnet(infoHash string) string {
	return "magnet:?xt=urn:btih:" + infoHash
}

type dhtNode interface {
	/* "Run" is called after calling "NewNode". */
	Run()

	/* "Create" and "Join" are called after calling "Run". */
	/* For a dhtNode, either "Create" or "Join" will be called, but not both. */
	Create()               /* Create a new network. */
	Join(addr string) bool /* Join an existing network. Return "true" if join succeeded and "false" if not. */

	/* Quit from the network it is currently in.*/
	/* "Quit" will not be called before "Create" or "Join". */
	/* For a dhtNode, "Quit" may be called for many times. */
	/* For a quited node, call "Quit" again should have no effect. */
	Quit()

	/* Chord offers a way of "normal" quitting. */
	/* For "force quit", the node quit the network without informing other nodes. */
	/* "ForceQuit" will be checked by TA manually. */
	ForceQuit()

	/* Check whether the node represented by the IP address is in the network. */
	Ping(addr string) bool

	/* Put a key-value pair into the network (if KEY is already in the network, cover it), or
	 * get a key-value pair from the network, or
	 * remove a key-value pair from the network.
	 */
	Put(key string, value string) bool /* Return "true" if success, "false" otherwise. */
	Get(key string) (bool, string)     /* Return "true" and the value if success, "false" otherwise. */
	Delete(key string) bool            /* Remove the key-value pair represented by KEY from the network. */
	/* Return "true" if remove successfully, "false" otherwise. */
}

func NewNode(port int) dhtNode {
	// Todo: create a node and then return it.
	node := new(kademlia.Node)
	node.Init(GetLocalAddress() + ":" + strconv.Itoa(port))
	return node
}

// function to get local address(ip address)
func GetLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				ipnet, ok := addr.(*net.IPNet)
				if ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}
