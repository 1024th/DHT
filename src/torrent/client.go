package torrent

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/jackpal/bencode-go"
)

type Client struct {
	addr string
	node dhtNode
}

func (c *Client) Init(port int) {
	c.node = NewNode(port)
	c.node.Run()
	yellow.Printf("Client start running at %s:%d\n", GetLocalAddress(), port)
}

func (c *Client) Create() {
	c.node.Create()
	green.Println("[success] Create new network.")
}

func (c *Client) Join(address string) {
	ok := c.node.Join(address)
	if ok {
		green.Printf("[success] Joined %s.\n", address)
	} else {
		red.Printf("[fail] Join %s failed.\n", address)
	}
}

func (c *Client) Quit() {
	c.node.Quit()
	green.Println("[success] Quit from current network.")
}

func (c *Client) DownLoadByMagnet(magnet string, savePath string) {
	ok, torrentStr := c.node.Get(magnet)
	if ok {
		reader := bytes.NewBufferString(torrentStr)
		torrent := bencodeTorrent{}
		err := bencode.Unmarshal(reader, &torrent)
		if err != nil {
			red.Printf("[fail] Unmarshal torrent error: %v\n", err)
		} else {
			torrent.Save("tmp.torrent")
			green.Println("[success] Save torrent file to tmp.torrent.")
		}
	} else {
		red.Println("[fail] Cannot find torrent of given magnet URL.")
	}
	c.DownLoadByTorrent("tmp.torrent", savePath)
}

func (c *Client) DownLoadByTorrent(torrentPath string, savePath string) {
	fileIO, err := os.Create(savePath)
	if err != nil {
		red.Printf("[fail] Create file %s error: %v.\n", savePath, err)
		return
	}
	torrent, err := Open(torrentPath)
	if err != nil {
		red.Printf("[fail] Failed to open torrent in path %s, error: %v.\n", torrentPath, err.Error())
		return
	}
	key := torrent.makeKeyPackage()
	yellow.Printf("File name: %s, size: %v bytes, piece num: %v.\n", torrent.Info.Name, torrent.Info.Length, key.size)
	fileName := torrent.Info.Name

	ok, data := c.DownLoad(&key)
	if ok {
		fileIO.Write(data)
		green.Printf("[success] Download %s to %s finished.", fileName, savePath)
	} else {
		red.Println("[fail] Download failed.")
	}
}

func (c *Client) Upload(filePath, torrentPath string) {
	yellow.Printf("Start uploading file %s ...\n", filePath)
	dataPackage, length := makeDataPackage(filePath)
	green.Println("[success] Data packaged, piece number: ", dataPackage.size)

	var pieces string
	for i := 0; i < dataPackage.size; i++ {
		piece, _ := PiecesHash(dataPackage.data[i], i)
		pieces += fmt.Sprintf("%x", piece)
	}
	_, fileName := path.Split(filePath)
	torrent := bencodeTorrent{
		Announce: "",
		Info: bencodeInfo{
			Length:      length,
			Pieces:      pieces,
			PieceLength: PieceSize,
			Name:        fileName,
		},
	}

	err := torrent.Save(torrentPath)
	if err != nil {
		red.Printf("[fail] Save torrent file error: %v.\n", err)
		return
	}

	key := torrent.makeKeyPackage()
	yellow.Printf("File name: %s, size: %v bytes, piece num: %v.\n", torrent.Info.Name, torrent.Info.Length, key.size)

	var buf bytes.Buffer
	err = bencode.Marshal(&buf, torrent)
	if err != nil {
		red.Printf("[fail] Torrent marshal error: %v.\n", err)
	}
	magnet := MakeMagnet(fmt.Sprintf("%x", key.infoHash))
	torrentStr := buf.String()

	ok := c.UploadPackage(&key, &dataPackage)
	if ok {
		yellow.Printf("Upload finished, create torrent file: %s\n", torrentPath)
		c.node.Put(magnet, torrentStr)
		yellow.Printf("Magnet URL: %s\n", magnet)
	}
}

type pieceWork struct {
	index   int
	retry   int
	success bool
	result  DataPiece
}

func (c *Client) uploadPieceWork(keyPackage *KeyPackage, dataPackage *DataPackage, index int, retry int, workQueue *chan pieceWork) {
	key := keyPackage.getKey(index)
	ok := c.node.Put(key, string(dataPackage.data[index]))
	if ok {
		*workQueue <- pieceWork{index: index, success: true, retry: retry}
	} else {
		*workQueue <- pieceWork{index: index, success: false, retry: retry}
	}
}

func (c *Client) UploadPackage(key *KeyPackage, data *DataPackage) bool {
	workQueue := make(chan pieceWork, WorkQueueBuffer)
	for i := 0; i < key.size; i++ {
		go c.uploadPieceWork(key, data, i, 0, &workQueue)
	}
	donePieces := 0
	for donePieces < key.size {
		select {
		case work := <-workQueue:
			if work.success {
				donePieces++
				green.Printf("[success] Piece #%d uploaded.\n", work.index+1)
			} else {
				red.Printf("[fail] Piece #%d upload failed %d/%d.\n", work.index+1, work.retry, UploadRetryNum)
				if work.retry < UploadRetryNum {
					go c.uploadPieceWork(key, data, work.index, work.retry+1, &workQueue)
				} else {
					red.Println("[fail] Upload failed.")
					return false
				}
			}
		case <-time.After(UploadTimeout * time.Duration(key.size)):
			red.Println("[fail] Upload timeout.")
			return false
		}
	}
	green.Println("[success] Upload finished.")
	return true
}

func (c *Client) downloadPieceWork(k *KeyPackage, index int, retry int, workQueue *chan pieceWork) {
	key := k.getKey(index)
	ok, piece := c.node.Get(key)
	if ok {
		*workQueue <- pieceWork{index: index, success: true, result: []byte(piece), retry: retry}
	} else {
		*workQueue <- pieceWork{index: index, success: false, retry: retry}
	}
}

func (c *Client) DownLoad(key *KeyPackage) (bool, []byte) {
	ret := make([]byte, key.length)
	workQueue := make(chan pieceWork, WorkQueueBuffer)
	for i := 0; i < key.size; i++ {
		go c.downloadPieceWork(key, i, 0, &workQueue)
	}

	donePieces := 0
	for donePieces < key.size {
		select {
		case work := <-workQueue:
			if work.success {
				donePieces++
				bound := (work.index + 1) * PieceSize
				if bound > key.length {
					bound = key.length
				}
				copy(ret[work.index*PieceSize:bound], work.result)
				pieceHash, _ := PiecesHash(work.result, work.index)
				if pieceHash != key.key[work.index] {
					red.Printf("[fail] Check integrity of Piece #%d failed.\n", work.index+1)
					return false, []byte{}
				}
				green.Printf("[success] Piece #%d downloaded.\n", work.index+1)
			} else {
				red.Printf("[fail] Piece #%d download failed %d/%d.\n", work.index+1, work.retry, DownloadRetryNum)
				if work.retry < DownloadRetryNum {
					go c.downloadPieceWork(key, work.index, work.retry+1, &workQueue)
				} else {
					red.Println("[fail] Download failed.")
					return false, []byte{}
				}
			}
		case <-time.After(DownloadTimeout * time.Duration(key.size)):
			red.Println("[fail] Download timeout.")
			return false, []byte{}

		}
	}

	green.Println("[success] File Downloaded.")
	return true, ret
}
