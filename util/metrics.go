package util

import (
	"time"

	"github.com/shirou/gopsutil/cpu"
)

func GetCpuUsage(sampleTime time.Duration) (float64, error) {
	first, err := cpu.Times(false)
	if err != nil {
		return 0, err
	}

	time.Sleep(sampleTime)

	second, err := cpu.Times(false)
	if err != nil {
		return 0, err
	}

	time.Sleep(sampleTime)

	third, err := cpu.Times(false)
	if err != nil {
		return 0, err
	}

	var totalPerc float64
	for i, _ := range first {
		firstUsage := calcUsage(first[i], second[i])
		secondUsage := calcUsage(second[i], third[i])
		totalPerc += (firstUsage + secondUsage) / 2
	}

	avg := totalPerc / float64(len(first))
	return avg, nil
}

func calcUsage(first cpu.TimesStat, second cpu.TimesStat) float64 {
	firstTotal := getTotalTime(first)
	secondTotal := getTotalTime(second)
	total := secondTotal - firstTotal
	idle := second.Idle - first.Idle
	return (total - idle) / total
}

func getTotalTime(t cpu.TimesStat) float64 {
	return t.User + t.System + t.Idle + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal
}
