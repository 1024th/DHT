package torrent

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

type KeyPiece [SHA1Len]byte
type DataPiece []byte

type DataPackage struct {
	size int
	data []DataPiece
}

type KeyPackage struct {
	size     int
	length   int
	infoHash [SHA1Len]byte
	key      []KeyPiece
}

func (this *KeyPackage) getKey(index int) string {
	var ret KeyPiece
	for i := 0; i < SHA1Len; i++ {
		ret[i] = this.key[index][i] ^ this.infoHash[i]
	}
	return fmt.Sprintf("%x", ret)
}

func makeDataPackage(path string) (DataPackage, int) {
	fileIO, err := os.Open(path)
	length := 0
	if err != nil {
		red.Printf("[fail] Open file in path %s err: %v.\n", path, err)
		return DataPackage{}, 0
	}

	var ret DataPackage
	for {
		buf := make([]byte, PieceSize)
		bufSize, err := fileIO.Read(buf)

		if err != nil && err != io.EOF {
			red.Printf("[fail] Read file error:%v.\n", err)
			return DataPackage{}, 0
		}
		if bufSize == 0 {
			break
		}
		ret.size++
		length += bufSize
		ret.data = append(ret.data, buf[:bufSize][:])
	}

	return ret, length
}

func (t *bencodeTorrent) makeKeyPackage() KeyPackage {
	var ret KeyPackage
	buf, _ := hex.DecodeString(t.Info.Pieces)

	ret.size = len(buf) / SHA1Len
	ret.infoHash, _ = t.Info.InfoHash()
	ret.length = t.Info.Length
	ret.key = make([]KeyPiece, ret.size)

	for i := 0; i < ret.size; i++ {
		copy(ret.key[i][:], buf[i*SHA1Len:(i+1)*SHA1Len])
	}
	return ret
}
