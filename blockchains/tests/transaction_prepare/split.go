package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	minOutputLovelace = uint64(1_000_000)

	feeBaseEstimate = uint64(600_000)
	feePerOutEst    = uint64(60_000)

	buildTimeout  = 2 * time.Minute
	signTimeout   = 1 * time.Minute
	submitTimeout = 30 * time.Second
	queryTimeout  = 30 * time.Second
	waitStep      = 1 * time.Second
	waitTotal     = 180 * time.Second // per-level wait

	maxAdjustAttempts  = 12
	adjustStepFraction = 50
)

type utxoEntry struct {
	TxIn     string
	Lovelace uint64
}

type addrFile struct{ Address string }

func readAddressFile(p string) (string, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return "", err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", errors.New("address file empty")
	}
	if s[0] == '{' {
		var a addrFile
		if err := json.Unmarshal(b, &a); err != nil {
			return "", fmt.Errorf("parse JSON address: %w", err)
		}
		if strings.TrimSpace(a.Address) == "" {
			return "", errors.New("address missing")
		}
		return strings.TrimSpace(a.Address), nil
	}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line, nil
		}
	}
	return "", errors.New("no address found")
}

func runCmd(ctx context.Context, name string, args ...string) (string, string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	return out.String(), errb.String(), err
}

func parseUtxoJSON(raw string) (map[string]uint64, error) {
	var m map[string]struct {
		Value map[string]any `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		if v.Value == nil || len(v.Value) != 1 {
			continue
		}
		val, ok := v.Value["lovelace"]
		if !ok {
			continue
		}
		switch t := val.(type) {
		case float64:
			if t >= 0 {
				out[k] = uint64(t)
			}
		case json.Number:
			u, _ := strconv.ParseUint(string(t), 10, 64)
			out[k] = u
		default:
			s := fmt.Sprint(val)
			if u, e := strconv.ParseUint(s, 10, 64); e == nil {
				out[k] = u
			}
		}
	}
	return out, nil
}

func queryAddrUtxos(ctx context.Context, socketPath string, magic int, addr string) (map[string]uint64, error) {
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	stdout, stderr, err := runCmd(qctx, "cardano-cli",
		"conway", "query", "utxo",
		"--socket-path", socketPath,
		"--testnet-magic", strconv.Itoa(magic),
		"--address", addr,
		"--output-json",
	)
	if err != nil {
		return nil, fmt.Errorf("query utxo failed: %v: %s", err, stderr)
	}
	return parseUtxoJSON(stdout)
}

func queryLargestAdaOnlyUTxO(ctx context.Context, socketPath string, magic int, addr string) (utxoEntry, error) {
	m, err := queryAddrUtxos(ctx, socketPath, magic, addr)
	if err != nil {
		return utxoEntry{}, err
	}
	var best utxoEntry
	for k, v := range m {
		if v >= best.Lovelace {
			best = utxoEntry{TxIn: k, Lovelace: v}
		}
	}
	if best.TxIn == "" {
		return utxoEntry{}, errors.New("no ADA-only UTxO found")
	}
	return best, nil
}

func estimateFeeBuffer(outs int) uint64 {
	return feeBaseEstimate + feePerOutEst*uint64(outs)
}

func containsValueNotConserved(stderr string) bool {
	s := strings.ToLower(stderr)
	return strings.Contains(s, "value not conserved") ||
		strings.Contains(s, "not enough ada") ||
		strings.Contains(s, "insufficient") ||
		(strings.Contains(s, "min") && strings.Contains(s, "utxo"))
}

func adjustShareDown(share uint64) uint64 {
	step := share / adjustStepFraction
	if step < 1_000 {
		step = 1_000
	}
	if share > step {
		return share - step
	}
	return share / 2
}

func buildTx(ctx context.Context, socketPath string, magic int, txIn, changeAddr, destAddr string, outs int, share uint64, body string) (string, string, error) {
	args := []string{"conway", "transaction", "build", "--tx-in", txIn}
	// Build tx-outs concurrently to reduce arg assembly time.
	type piece struct{ a []string }
	ch := make(chan piece, outs)
	workers := minInt(outs, runtime.NumCPU())
	var wg sync.WaitGroup
	wg.Add(workers)
	idx := make(chan struct{}, outs)
	for i := 0; i < outs; i++ {
		idx <- struct{}{}
	}
	close(idx)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for range idx {
				ch <- piece{a: []string{"--tx-out", fmt.Sprintf("%s+%d", destAddr, share)}}
			}
		}()
	}
	go func() { wg.Wait(); close(ch) }()
	for p := range ch {
		args = append(args, p.a...)
	}
	args = append(args,
		"--change-address", changeAddr,
		"--testnet-magic", strconv.Itoa(magic),
		"--socket-path", socketPath,
		"--out-file", body,
	)
	bctx, cancel := context.WithTimeout(ctx, buildTimeout)
	defer cancel()
	return runCmd(bctx, "cardano-cli", args...)
}

func signTx(ctx context.Context, magic int, skeyFile, bodyFile, outSigned string) error {
	sctx, cancel := context.WithTimeout(ctx, signTimeout)
	defer cancel()
	_, stderr, err := runCmd(sctx, "cardano-cli", "conway", "transaction", "sign",
		"--signing-key-file", skeyFile,
		"--testnet-magic", strconv.Itoa(magic),
		"--tx-body-file", bodyFile,
		"--out-file", outSigned,
	)
	if err != nil {
		return fmt.Errorf("sign failed: %v: %s", err, stderr)
	}
	return nil
}

func submitTx(ctx context.Context, socketPath string, magic int, signedFile string) error {
	uctx, cancel := context.WithTimeout(ctx, submitTimeout)
	defer cancel()
	_, stderr, err := runCmd(uctx, "cardano-cli", "conway", "transaction", "submit",
		"--tx-file", signedFile,
		"--testnet-magic", strconv.Itoa(magic),
		"--socket-path", socketPath,
	)
	if err != nil {
		return fmt.Errorf("submit failed: %v: %s", err, stderr)
	}
	return nil
}

func txID(ctx context.Context, signedFile string) (string, error) {
	tctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	stdout, stderr, err := runCmd(tctx, "cardano-cli", "conway", "transaction", "txid",
		"--tx-file", signedFile,
	)
	if err != nil {
		return "", fmt.Errorf("txid failed: %v: %s", err, stderr)
	}
	return strings.TrimSpace(stdout), nil
}

func waitForOutputs(ctx context.Context, socketPath string, magic int, addr, txid string, want int) ([]string, error) {
	deadline := time.Now().Add(waitTotal)
	var have []string
	for time.Now().Before(deadline) {
		m, err := queryAddrUtxos(ctx, socketPath, magic, addr)
		if err != nil {
			return nil, err
		}
		have = have[:0]
		for k := range m {
			if strings.HasPrefix(k, txid+"#") {
				have = append(have, k)
			}
		}
		if len(have) >= want {
			// deterministic order by index
			sort.Slice(have, func(i, j int) bool {
				ix := strings.Split(strings.TrimPrefix(have[i], txid+"#"), "#")[0]
				jx := strings.Split(strings.TrimPrefix(have[j], txid+"#"), "#")[0]
				ii, _ := strconv.Atoi(ix)
				jj, _ := strconv.Atoi(jx)
				return ii < jj
			})
			return have[:want], nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(waitStep):
		}
	}
	return nil, fmt.Errorf("timeout waiting for %d outputs of %s", want, txid)
}

func buildSignSubmitSplit(ctx context.Context, socket, skey string, magic int, seedTxIn, seedAddr, destAddr string, outs int) (txid string, childTxIns []string, err error) {
	// Determine available lovelace on the seed.
	// All seeds live at seedAddr; find the specific TXin.
	m, err := queryAddrUtxos(ctx, socket, magic, seedAddr)
	if err != nil {
		return "", nil, err
	}
	amt, ok := m[seedTxIn]
	if !ok {
		return "", nil, fmt.Errorf("seed %s not found at %s", seedTxIn, seedAddr)
	}
	if amt < minOutputLovelace*uint64(outs) {
		outs = int(amt / minOutputLovelace)
		if outs == 0 {
			return "", nil, fmt.Errorf("seed %s too small (%d)", seedTxIn, amt)
		}
	}
	bodyDir, _ := os.MkdirTemp("", "splitlvl-*")
	defer os.RemoveAll(bodyDir)
	body := filepath.Join(bodyDir, "tx.body")
	signed := filepath.Join(bodyDir, "tx.signed")

	feeBuf := estimateFeeBuffer(outs)
	share := uint64(0)
	if amt > feeBuf {
		share = (amt - feeBuf) / uint64(outs)
	}
	if share < minOutputLovelace {
		share = minOutputLovelace
	}

	var stderr string
	for attempt := 0; attempt < maxAdjustAttempts; attempt++ {
		_, stderr, err = buildTx(ctx, socket, magic, seedTxIn, seedAddr, destAddr, outs, share, body)
		if err == nil {
			break
		}
		if !containsValueNotConserved(stderr) {
			return "", nil, fmt.Errorf("build failed: %v: %s", err, stderr)
		}
		share = adjustShareDown(share)
		if share < minOutputLovelace {
			return "", nil, fmt.Errorf("cannot satisfy min output after adjustments")
		}
	}
	if err != nil {
		return "", nil, fmt.Errorf("build failed after adjustments: %v: %s", err, stderr)
	}
	if err := signTx(ctx, magic, skey, body, signed); err != nil {
		return "", nil, err
	}
	if err := submitTx(ctx, socket, magic, signed); err != nil {
		return "", nil, err
	}
	id, err := txID(ctx, signed)
	if err != nil {
		return "", nil, err
	}
	// Collect child TXins if we will split them later (i.e., destAddr == seedAddr).
	if destAddr == seedAddr {
		children, err := waitForOutputs(ctx, socket, magic, destAddr, id, outs)
		if err != nil {
			return id, nil, err
		}
		return id, children, nil
	}
	return id, nil, nil
}

type levelPlan struct {
	Levels int
	Cap    int
	// For intermediate levels: fixed branching.
	Branch []int
	// For final level: per-seed fanout counts.
	FinalPerSeed []int
}

// planBalanced chooses L and branching near N^(1/L) with per-tx cap.
func planBalanced(N, cap int) levelPlan {
	if N <= cap {
		return levelPlan{Levels: 1, Cap: cap, Branch: nil, FinalPerSeed: []int{N}}
	}
	// Find smallest L with b^L >= N where b = min(cap, ceil(N^(1/L))).
	L := 1
	for {
		L++
		b := int(math.Ceil(math.Pow(float64(N), 1.0/float64(L))))
		if b > cap {
			b = cap
		}
		if intPow(b, L) >= N {
			break
		}
	}
	b := int(math.Ceil(math.Pow(float64(N), 1.0/float64(L))))
	if b > cap {
		b = cap
	}
	branch := make([]int, L-1)
	for i := range branch {
		branch[i] = b
	}
	seeds := intPow(b, L-1)
	q := N / seeds
	r := N % seeds
	if q == 0 {
		// Too many seeds; reduce last intermediate branching.
		// Fall back to sequential batches.
		return levelPlan{Levels: 1, Cap: cap, Branch: nil, FinalPerSeed: []int{N}}
	}
	final := make([]int, seeds)
	for i := 0; i < seeds; i++ {
		v := q
		if i < r {
			v++
		}
		if v > cap {
			v = cap
		}
		final[i] = v
	}
	return levelPlan{Levels: L, Cap: cap, Branch: branch, FinalPerSeed: final}
}

func intPow(a, b int) int {
	res := 1
	for i := 0; i < b; i++ {
		res *= a
	}
	return res
}

func workerPool[T any, R any](ctx context.Context, items []T, limit int, fn func(context.Context, int, T) (R, error)) ([]R, error) {
	type taskRes struct {
		idx int
		val R
		err error
	}
	out := make([]R, len(items))
	sem := make(chan struct{}, maxInt(1, limit))
	errCh := make(chan taskRes, len(items))
	var wg sync.WaitGroup
	for i := range items {
		i := i
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			v, e := fn(ctx, i, items[i])
			errCh <- taskRes{idx: i, val: v, err: e}
		}()
	}
	wg.Wait()
	close(errCh)
	for tr := range errCh {
		if tr.err != nil {
			return nil, tr.err
		}
		out[tr.idx] = tr.val
	}
	return out, nil
}

// SplitEqualPlanned executes the multi-level plan with per-level parallelism.
func SplitEqualPlanned(ctx context.Context, socketPath string, magic int, skeyFile, inputAddrFile, outputAddrFile string, splits, cap, threads int) error {
	if splits <= 0 {
		return errors.New("splits must be > 0")
	}
	inAddr, err := readAddressFile(inputAddrFile)
	if err != nil {
		return fmt.Errorf("input address: %w", err)
	}
	outAddr, err := readAddressFile(outputAddrFile)
	if err != nil {
		return fmt.Errorf("output address: %w", err)
	}

	seed, err := queryLargestAdaOnlyUTxO(ctx, socketPath, magic, inAddr)
	if err != nil {
		return err
	}

	plan := planBalanced(splits, cap)
	fmt.Printf("plan: levels=%d branch=%v finalSeeds=%d\n", plan.Levels, plan.Branch, len(plan.FinalPerSeed))

	// Gather seeds per level. Level 0 starts with the original seed.
	seeds := []string{seed.TxIn}

	// Intermediate levels
	for lvl := 0; lvl < plan.Levels-1; lvl++ {
		b := plan.Branch[lvl]
		fmt.Printf("level %d: seeds=%d -> fanout=%d each -> nextSeeds=%d\n", lvl+1, len(seeds), b, len(seeds)*b)

		type job struct {
			TxIn string
		}
		jobs := make([]job, len(seeds))
		for i, s := range seeds {
			jobs[i] = job{TxIn: s}
		}
		type out struct{ children []string }
		res, err := workerPool(ctx, jobs, threads, func(c context.Context, _ int, j job) (out, error) {
			_, kids, e := buildSignSubmitSplit(c, socketPath, skeyFile, magic, j.TxIn, inAddr, inAddr, b)
			return out{children: kids}, e
		})
		if err != nil {
			return fmt.Errorf("level %d failed: %w", lvl+1, err)
		}
		next := make([]string, 0, len(seeds)*b)
		for _, r := range res {
			next = append(next, r.children...)
		}
		seeds = next
	}

	// Final level: distribute target counts across seeds.
	if plan.Levels == 1 {
		// Single transaction path.
		_, _, err := buildSignSubmitSplit(ctx, socketPath, skeyFile, magic, seeds[0], inAddr, outAddr, splits)
		return err
	}
	fmt.Printf("final level: seeds=%d, total leaves=%d\n", len(seeds), splits)
	if len(seeds) != len(plan.FinalPerSeed) {
		return fmt.Errorf("internal plan mismatch: have %d seeds, planned %d", len(seeds), len(plan.FinalPerSeed))
	}

	type fjob struct {
		TxIn string
		N    int
	}
	fjobs := make([]fjob, len(seeds))
	for i := range seeds {
		fjobs[i] = fjob{TxIn: seeds[i], N: plan.FinalPerSeed[i]}
	}
	_, err = workerPool(ctx, fjobs, threads, func(c context.Context, _ int, j fjob) (struct{}, error) {
		_, _, e := buildSignSubmitSplit(c, socketPath, skeyFile, magic, j.TxIn, inAddr, outAddr, j.N)
		return struct{}{}, e
	})
	return err
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
