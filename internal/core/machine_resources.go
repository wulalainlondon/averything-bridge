package core

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type machineResourceSnapshot struct {
	InstanceID         string  `json:"instance_id"`
	SampledAtMS        int64   `json:"sampled_at_ms"`
	MemoryTotalBytes   *uint64 `json:"memory_total_bytes"`
	MemoryUsedBytes    *uint64 `json:"memory_used_bytes"`
	DiskTotalBytes     *uint64 `json:"disk_total_bytes"`
	DiskUsedBytes      *uint64 `json:"disk_used_bytes"`
	DiskAvailableBytes *uint64 `json:"disk_available_bytes"`
}

var machineResourceCache struct {
	sync.Mutex
	value machineResourceSnapshot
	until time.Time
}

func resourceNumber(text string) (uint64, bool) {
	n, e := strconv.ParseUint(strings.TrimSpace(text), 10, 64)
	return n, e == nil
}
func resourcePointer(n uint64) *uint64 { return &n }
func parseResourceDF(text string) (total, used, available *uint64) {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) < 2 {
		return
	}
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 6 {
		return
	}
	a, ok := resourceNumber(fields[1])
	b, ok2 := resourceNumber(fields[2])
	c, ok3 := resourceNumber(fields[3])
	if !ok || !ok2 || !ok3 || a == 0 || a > 1<<53/1024 || b > a || c > a || b+c > a {
		return
	}
	return resourcePointer(a * 1024), resourcePointer(b * 1024), resourcePointer(c * 1024)
}
func parseDarwinResourceMemory(total uint64, text string) *uint64 {
	if total == 0 {
		return nil
	}
	lines := strings.Split(text, "\n")
	if len(lines) < 2 {
		return nil
	}
	header := strings.Fields(lines[0])
	var pageSize uint64
	for i, w := range header {
		if w == "size" && i+2 < len(header) {
			pageSize, _ = resourceNumber(header[i+2])
			break
		}
	}
	if pageSize == 0 || pageSize > 65536 {
		return nil
	}
	values := map[string]uint64{}
	for _, line := range lines[1:] {
		k, v, ok := strings.Cut(line, ":")
		if ok {
			if n, valid := resourceNumber(strings.TrimSuffix(strings.TrimSpace(v), ".")); valid {
				values[strings.TrimSpace(k)] = n
			}
		}
	}
	var pages uint64
	for _, k := range []string{"Pages active", "Pages wired down", "Pages occupied by compressor"} {
		v, ok := values[k]
		if !ok || v > total/pageSize {
			return nil
		}
		pages += v
	}
	if pages > total/pageSize {
		return nil
	}
	return resourcePointer(pages * pageSize)
}
func readMachineResources(instanceID string) machineResourceSnapshot {
	machineResourceCache.Lock()
	defer machineResourceCache.Unlock()
	if time.Now().Before(machineResourceCache.until) {
		v := machineResourceCache.value
		v.InstanceID = instanceID
		return v
	}
	v := machineResourceSnapshot{SampledAtMS: time.Now().UnixMilli()}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	switch runtime.GOOS {
	case "darwin":
		total, e := exec.CommandContext(ctx, "/usr/sbin/sysctl", "-n", "hw.memsize").Output()
		if e == nil {
			if n, ok := resourceNumber(string(total)); ok && n > 0 && n <= 1<<53 {
				v.MemoryTotalBytes = resourcePointer(n)
				if data, e := exec.CommandContext(ctx, "/usr/bin/vm_stat").Output(); e == nil {
					v.MemoryUsedBytes = parseDarwinResourceMemory(n, string(data))
				}
			}
		}
	case "linux":
		if data, e := os.ReadFile("/proc/meminfo"); e == nil && len(data) < 65536 {
			fields := map[string]uint64{}
			for _, line := range strings.Split(string(data), "\n") {
				p := strings.Fields(line)
				if len(p) == 3 && p[2] == "kB" {
					if n, ok := resourceNumber(p[1]); ok && n <= 1<<53/1024 {
						fields[strings.TrimSuffix(p[0], ":")] = n * 1024
					}
				}
			}
			total, ok := fields["MemTotal"]
			available, ok2 := fields["MemAvailable"]
			if ok && ok2 && total > 0 && available <= total {
				v.MemoryTotalBytes = resourcePointer(total)
				v.MemoryUsedBytes = resourcePointer(total - available)
			}
		}
	}
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
		if home, e := os.UserHomeDir(); e == nil {
			if data, e := exec.CommandContext(ctx, "/bin/df", "-Pk", home).Output(); e == nil {
				v.DiskTotalBytes, v.DiskUsedBytes, v.DiskAvailableBytes = parseResourceDF(string(data))
			}
		}
	}
	machineResourceCache.value = v
	machineResourceCache.until = time.Now().Add(5 * time.Second)
	v.InstanceID = instanceID
	return v
}
