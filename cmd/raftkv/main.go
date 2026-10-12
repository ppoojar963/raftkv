// Command raftkv runs one replica of the key-value store.
//
//	raftkv -id 1 -cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 -data ./data/1
//
// Client API (on the same address as the Raft RPCs):
//
//	GET    /kv/{key}   -> 200 value | 404
//	PUT    /kv/{key}   -> 204        (request body is the value)
//	DELETE /kv/{key}   -> 204
//	GET    /status     -> node role, term, and leader
//
// Non-leaders answer 307 with the leader's URL, so `curl -L` just works.
// Send X-Client-ID and X-Request-Seq headers to make retried writes exactly-once.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ppoojar963/raftkv/kv"
	"github.com/ppoojar963/raftkv/raft"
	"github.com/ppoojar963/raftkv/transport"
)

func main() {
	id := flag.Int("id", 1, "this node's ID")
	clusterSpec := flag.String("cluster", "1=127.0.0.1:7001", "comma-separated id=host:port for every node")
	dataDir := flag.String("data", "", "directory for persistent state (default ./data/<id>)")
	flag.Parse()

	addrs, err := parseCluster(*clusterSpec)
	if err != nil {
		log.Fatal(err)
	}
	self, ok := addrs[*id]
	if !ok {
		log.Fatalf("id %d not in -cluster", *id)
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("./data/%d", *id)
	}
	peers := make([]int, 0, len(addrs))
	for p := range addrs {
		peers = append(peers, p)
	}
	sort.Ints(peers)

	storage, err := raft.NewWALStorage(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	store, err := kv.NewStore(raft.DefaultConfig(*id, peers), transport.NewHTTP(addrs), storage)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	transport.Register(mux, store.Node())
	api := &api{store: store, addrs: addrs, id: *id}
	mux.HandleFunc("GET /kv/{key}", api.handle(kv.OpGet))
	mux.HandleFunc("PUT /kv/{key}", api.handle(kv.OpPut))
	mux.HandleFunc("DELETE /kv/{key}", api.handle(kv.OpDelete))
	mux.HandleFunc("GET /status", api.status)

	srv := &http.Server{Addr: self, Handler: mux}
	store.Start()
	go func() {
		log.Printf("node %d listening on %s (peers %v, data %s)", *id, self, peers, *dataDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	store.Stop()
	log.Printf("node %d stopped", *id)
}

type api struct {
	store *kv.Store
	addrs map[int]string
	id    int
}

func (a *api) handle(t kv.OpType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		op := kv.Op{Type: t, Key: r.PathValue("key"), ClientID: r.Header.Get("X-Client-ID")}
		if op.ClientID != "" {
			seq, err := strconv.ParseInt(r.Header.Get("X-Request-Seq"), 10, 64)
			if err != nil || seq <= 0 {
				http.Error(w, "X-Request-Seq must be a positive integer when X-Client-ID is set", http.StatusBadRequest)
				return
			}
			op.Seq = seq
		}
		if t == kv.OpPut {
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			op.Value = string(body)
		}

		res, err := a.store.Submit(op)
		var nl *kv.NotLeaderError
		switch {
		case errors.As(err, &nl):
			if addr, ok := a.addrs[nl.LeaderID]; ok {
				http.Redirect(w, r, "http://"+addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
				return
			}
			http.Error(w, "no leader elected yet; retry", http.StatusServiceUnavailable)
		case err != nil:
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		case t != kv.OpGet:
			w.WriteHeader(http.StatusNoContent)
		case !res.Found:
			http.Error(w, "not found", http.StatusNotFound)
		default:
			io.WriteString(w, res.Value)
		}
	}
}

func (a *api) status(w http.ResponseWriter, _ *http.Request) {
	term, isLeader, leader := a.store.Node().Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id": a.id, "term": term, "is_leader": isLeader, "leader": leader,
	})
}

func parseCluster(spec string) (map[int]string, error) {
	out := map[int]string{}
	for _, part := range strings.Split(spec, ",") {
		idStr, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("bad -cluster entry %q (want id=host:port)", part)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("bad node id %q", idStr)
		}
		out[id] = addr
	}
	return out, nil
}
