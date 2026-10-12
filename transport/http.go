// Package transport carries Raft RPCs between processes over HTTP/JSON.
package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ppoojar963/raftkv/raft"
)

type HTTP struct {
	addrs  map[int]string // node ID -> "host:port"
	client *http.Client
}

func NewHTTP(addrs map[int]string) *HTTP {
	return &HTTP{
		addrs: addrs,
		// Well under the election timeout, so an unresponsive peer can't stall a round.
		client: &http.Client{Timeout: 150 * time.Millisecond},
	}
}

func (h *HTTP) call(to int, path string, args, reply any) error {
	addr, ok := h.addrs[to]
	if !ok {
		return fmt.Errorf("transport: unknown peer %d", to)
	}
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	resp, err := h.client.Post("http://"+addr+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("transport: %s from peer %d", resp.Status, to)
	}
	return json.NewDecoder(resp.Body).Decode(reply)
}

func (h *HTTP) RequestVote(to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var r raft.RequestVoteReply
	return &r, h.call(to, "/raft/vote", args, &r)
}

func (h *HTTP) AppendEntries(to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var r raft.AppendEntriesReply
	return &r, h.call(to, "/raft/append", args, &r)
}

// Register mounts the RPC endpoints for node on mux.
func Register(mux *http.ServeMux, node *raft.Node) {
	mux.HandleFunc("POST /raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var args raft.RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(node.HandleRequestVote(&args))
	})
	mux.HandleFunc("POST /raft/append", func(w http.ResponseWriter, r *http.Request) {
		var args raft.AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(node.HandleAppendEntries(&args))
	})
}
