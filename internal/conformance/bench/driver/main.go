package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

const hello = "Hello Benchmark"
const request = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"

type process struct {
	Address, Admin string
	PID            int
	command        *exec.Cmd
	cancel         context.CancelFunc
}

func launch(bin, impl string, track bool, profileDir string) (*process, error) {
	args := []string{"-impl", impl, "-track-states=" + fmt.Sprint(track)}
	if profileDir != "" {
		args = append(args, "-cpuprofile", filepath.Join(profileDir, impl+".cpu.pprof"),
			"-mutexprofile", filepath.Join(profileDir, impl+".mutex.pprof"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stderr = os.Stderr
	output, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}
	p := &process{command: cmd, cancel: cancel}
	if err := json.NewDecoder(bufio.NewReader(output)).Decode(p); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		cancel()
		return nil, fmt.Errorf("read server readiness: %w", err)
	}
	fmt.Printf("SERVER impl=%s pid=%d address=%s admin=%s started=%s\n", impl, p.PID, p.Address, p.Admin, time.Now().UTC().Format(time.RFC3339Nano))
	return p, nil
}

func (p *process) close() error {
	defer p.cancel()
	err := p.command.Process.Signal(syscall.SIGTERM)
	waitErr := p.command.Wait()
	fmt.Printf("CLEANUP server pid=%d joined; listeners, workers and timers closed\n", p.PID)
	return errors.Join(err, waitErr)
}

func sample(p *process) error {
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	for range 20 {
		res, err := client.Get("http://" + p.Address + "/")
		if err != nil {
			return err
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil || res.StatusCode != 200 || string(body) != hello {
			return fmt.Errorf("body validation status=%d bytes=%q error=%v", res.StatusCode, body, err)
		}
	}
	return nil
}

func bombard(binary string, p *process, connections int, duration time.Duration) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), duration+30*time.Second)
	defer cancel()
	args := []string{"-c", fmt.Sprint(connections), "-d", duration.String(), "-o", "json", "-p", "r", "http://" + p.Address + "/"}
	fmt.Printf("COMMAND %s %v\n", binary, args)
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	fmt.Printf("BOMBARDIER %s\n", out)
	var report struct {
		Result struct {
			RPS                                            struct{ Mean float64 }
			Req1xx, Req2xx, Req3xx, Req4xx, Req5xx, Others uint64
			Errors                                         []struct{ Count uint64 }
		}
	}
	if err := json.Unmarshal(out, &report); err != nil {
		return 0, err
	}
	result := report.Result
	bad := result.Req1xx + result.Req3xx + result.Req4xx + result.Req5xx + result.Others
	for _, e := range result.Errors {
		bad += e.Count
	}
	if bad != 0 || result.Req2xx == 0 || result.RPS.Mean <= 0 {
		return 0, fmt.Errorf("invalid bombardier row requests=%d errors=%d rps=%f", result.Req2xx, bad, result.RPS.Mean)
	}
	return result.RPS.Mean, sample(p)
}

func median(values []float64) float64 {
	values = append([]float64(nil), values...)
	sort.Float64s(values)
	return values[len(values)/2]
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}

func run() error {
	server := flag.String("server", "", "benchserver binary")
	bomb := flag.String("bombardier", "", "bombardier binary")
	wsclient := flag.String("wsclient", "", "optional validated WebSocket client binary")
	duration := flag.Duration("duration", 15*time.Second, "duration per throughput run")
	runs := flag.Int("runs", 3, "runs per implementation")
	connections := flag.Int("connections", 125, "load connections")
	platform := flag.String("platform", "", "measurement platform")
	profile := flag.String("profile-dir", "", "optional server profiles, exploratory only")
	flag.Parse()
	if *server == "" || *bomb == "" || *runs <= 0 || *connections <= 0 || *duration <= 0 {
		return fmt.Errorf("invalid benchmark arguments")
	}
	helloRPS, pipelineRPS := map[string][]float64{}, map[string][]float64{}
	for trial := 1; trial <= *runs; trial++ {
		for _, impl := range []string{"std", "hoe"} {
			if err := func() (err error) {
				p, err := launch(*server, impl, false, *profile)
				if err != nil {
					return err
				}
				defer func() {
					if e := p.close(); err == nil {
						err = e
					}
				}()
				rps, err := bombard(*bomb, p, *connections, *duration)
				if err != nil {
					return err
				}
				helloRPS[impl] = append(helloRPS[impl], rps)
				fmt.Printf("ROW hello platform=%s impl=%s c=%d trial=%d rps=%.2f errors=0 body_samples=20\n", *platform, impl, *connections, trial, rps)
				rps, err = pipeline(p.Address, *connections, 16, *duration)
				if err != nil {
					return err
				}
				pipelineRPS[impl] = append(pipelineRPS[impl], rps)
				fmt.Printf("ROW pipeline16 platform=%s impl=%s c=%d trial=%d rps=%.2f errors=0 all_bodies_validated=true\n", *platform, impl, *connections, trial, rps)
				return nil
			}(); err != nil {
				return err
			}
		}
	}
	fmt.Println("CLAIM platform case std_median_rps hoe_median_rps ratio errors body_validation")
	for _, row := range []struct {
		name string
		data map[string][]float64
	}{{"hello", helloRPS}, {"pipeline16", pipelineRPS}} {
		std, own := median(row.data["std"]), median(row.data["hoe"])
		fmt.Printf("CLAIM %s %s %.2f %.2f %.4f 0 PASS\n", *platform, row.name, std, own, own/std)
	}
	for _, impl := range []string{"std", "hoe"} {
		if err := resources(*server, impl, *wsclient); err != nil {
			return err
		}
	}
	if *platform == "linux" && *runs == 3 && *duration == 15*time.Second {
		if median(helloRPS["hoe"]) < median(helloRPS["std"]) || median(pipelineRPS["hoe"]) <= median(pipelineRPS["std"]) {
			return fmt.Errorf("Linux throughput acceptance failed")
		}
	}
	fmt.Println("PASS every row has zero errors and validated bodies")
	return nil
}
