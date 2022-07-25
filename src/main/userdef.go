package main

import (
	"chord"
	"strconv"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

func NewNode(port int) dhtNode {
	// Todo: create a node and then return it.
	node := new(chord.Node)
	node.Initialize(GetLocalAddress() + ":" + strconv.Itoa(port))
	return node
}

// Todo: implement a struct which implements the interface "dhtNode".
