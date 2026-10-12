// Command bench measures raftkv throughput and latency on a real cluster:
// N nodes in one process, talking Raft over HTTP on localhost, each with its
// own fsync'd WAL. C concurrent clients issue PUTs against the leader.
//
//	go run ./cmd/bench -nodes 1,3,5 -clients 1,16,64 -duration 5s
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ppoojar963/raftkv/kv"
	"github.com/ppoojar963/raftkv/raft"
	"github.com/ppoojar963/raftkv/transport"
)

func main() {
	nodesFlag := flag.String("nodes", "1,3,5", "cluster sizes")
	clientsFlag := flag.String("clients", "1,16,64", "concurrent clients")
	duration := flag.Duration("duration", 5*time.Second, "measurement time per configuration")
	valueSize := flag.Int("value", 128, "value size in bytes")
	flag.Parse()

	fmt.Printf("| Nodes | Clients | Throughput (ops/s) | p50 | p99 | p99.9 |\n|---|---|---|---|---|---|\n")
	for _, n := range ints(*nodesFlag) {
		for _, c := range ints(*clientsFlag) {
			r := run(n, c, *duration, *valueSize)
			fmt.Printf("| %d | %d | %s | %s | %s | %s |\n", n, c, commas(r.tput), ms(r.p50), ms(r.p99), ms(r.p999))
		}
	}
}

type result struct {
	tput           float64
	p50, p99, p999 time.Duration
}

func run(n, clients int, d time.Duration, valueSize int) result {
	dir, _ := os.MkdirTemp("", "raftkv-bench")
	defer os.RemoveAll(dir)

	addrs := map[int]string{}
	lns := map[int]net.Listener{}
	peers := []int{}
	for i := 1; i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatal(err)
		}
		lns[i], addrs[i] = ln, ln.Addr().String()
		peers = append(peers, i)
	}
	stores := map[int]*kv.Store{}
	var servers []*http.Server
	for _, i := range peers {
		wal, err := raft.NewWALStorage(fmt.Sprintf("%s/%d", dir, i))
		if err != nil {
			log.Fatal(err)
		}
		s, err := kv.NewStore(raft.DefaultConfig(i, peers), transport.NewHTTP(addrs), wal)
		if err != nil {
			log.Fatal(err)
		}
		s.Timeout = 5 * time.Second
		mux := http.NewServeMux()
		transport.Register(mux, s.Node())
		srv := &http.Server{Handler: mux}
		go srv.Serve(lns[i])
		servers = append(servers, srv)
		s.Start()
		stores[i] = s
		defer wal.Close()
	}
	defer func() {
		for _, srv := range servers {
			srv.Close()
		}
		for _, s := range stores {
			s.Stop()
		}
	}()

	var leader *kv.Store
	for leader == nil {
		time.Sleep(50 * time.Millisecond)
		for _, s := range stores {
			if _, ok, _ := s.Node().Status(); ok {
				leader = s
			}
		}
	}
	value := strings.Repeat("x", valueSize)

	var ops atomic.Int64
	lats := make([][]time.Duration, clients)
	var wg sync.WaitGroup
	warmupEnd := time.Now().Add(time.Second)
	end := warmupEnd.Add(d)
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			id := fmt.Sprintf("bench-%d", c)
			for seq := int64(1); ; seq++ {
				t0 := time.Now()
				if t0.After(end) {
					return
				}
				_, err := leader.Submit(kv.Op{Type: kv.OpPut, Key: fmt.Sprintf("k%d", seq%1000), Value: value, ClientID: id, Seq: seq})
				if err != nil {
					log.Fatalf("submit: %v", err)
				}
				if t0.After(warmupEnd) {
					lats[c] = append(lats[c], time.Since(t0))
					ops.Add(1)
				}
			}
		}(c)
	}
	wg.Wait()

	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration { return all[int(p*float64(len(all)-1))] }
	return result{float64(ops.Load()) / d.Seconds(), pct(0.50), pct(0.99), pct(0.999)}
}

func ints(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			log.Fatalf("bad number %q", p)
		}
		out = append(out, v)
	}
	return out
}

func ms(d time.Duration) string { return fmt.Sprintf("%.2f ms", float64(d.Microseconds())/1000) }

func commas(f float64) string {
	s := strconv.Itoa(int(f))
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
