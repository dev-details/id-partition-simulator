package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/progress"
	"io"
	"math"
	"os"
	"strconv"
	"time"
)

type Partition struct {
	current int64
	max     int64
	errored bool
}

type Config struct {
	partitions int
	max        int64
	min        int64
	file       *os.File
	writer     progress.Writer
	fileSize   int64
	iterations int
	outlier    int64
}

type Result struct {
	totalDocuments  int64
	totalIDs        int64
	partitionCounts []int64
}

func main() {
	config := parseFlags()
	defer config.file.Close()

	go config.writer.Render()

	for !config.writer.IsRenderInProgress() {
		// Wait for renderer to start
		time.Sleep(10 * time.Millisecond)
	}

	if _, err := ProcessCSV(config); err != nil {
		fmt.Printf("Error processing CSV: %v\n", err)
		os.Exit(1)
	}

	config.writer.Stop()
}

func parseFlags() Config {
	config := Config{}
	flag.IntVar(&config.partitions, "partitions", 1, "Number of partitions")
	flag.Int64Var(&config.max, "max", math.MaxInt64, "Maximum value for each partition")
	flag.Int64Var(&config.min, "min", 1, "Minimum value for each partition")
	flag.IntVar(&config.iterations, "iterations", 1, "Number of times to process the CSV file")
	flag.Int64Var(&config.outlier, "outlier", math.MaxInt64, "Outlier threshold for document count. Documents with more than this count are logged and skipped.")

	var csvFile string
	flag.StringVar(&csvFile, "file", "", "CSV file to process")
	flag.Parse()

	if csvFile == "" {
		fmt.Println("Please provide a CSV file name using the -file flag")
		os.Exit(1)
	}

	file, err := os.Open(csvFile)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("Error getting file info: %v\n", err)
		os.Exit(1)
	}
	config.fileSize = fileInfo.Size()
	config.file = file

	config.writer = progress.NewWriter()
	config.writer.SetTrackerPosition(progress.PositionRight)
	config.writer.SetAutoStop(true)
	config.writer.SetTrackerLength(50)
	config.writer.SetMessageLength(15)
	config.writer.SetNumTrackersExpected(config.partitions + 1)
	config.writer.SetSortBy(progress.SortByMessage)
	config.writer.SetStyle(progress.StyleBlocks)
	config.writer.Style().Colors = progress.StyleColorsExample
	config.writer.Style().Visibility.Time = false
	config.writer.Style().Visibility.Value = true

	return config
}

func ProcessCSV(config Config) (Result, error) {
	partitionList := make([]Partition, config.partitions)
	for i := range partitionList {
		partitionList[i] = Partition{current: config.min, max: config.max, errored: false}
	}

	overallTracker := &progress.Tracker{
		Message: "Total Progress",
		Total:   config.fileSize * int64(config.iterations),
		Units:   progress.UnitsBytes,
	}
	config.writer.AppendTracker(overallTracker)

	partitionTrackers := make([]*progress.Tracker, config.partitions)
	for i := range partitionTrackers {
		partitionTrackers[i] = &progress.Tracker{
			Message: fmt.Sprintf("Partition %03d", i),
			Total:   config.max - config.min,
			Units:   progress.UnitsDefault,
		}
		config.writer.AppendTracker(partitionTrackers[i])
	}

	result := Result{
		totalDocuments:  int64(0),
		totalIDs:        int64(0),
		partitionCounts: make([]int64, config.partitions),
	}

	erroredPartitions := 0
	var pinnedMessages []string
	loggedOutliers := make(map[uint32]bool)
	overallPosition := int64(0)

	for iteration := 1; iteration <= config.iterations; iteration++ {
		position, err := config.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return result, fmt.Errorf("error getting file position: %w", err)
		}
		overallPosition += position

		_, err = config.file.Seek(0, 0) // Reset file pointer to the beginning
		if err != nil {
			return result, fmt.Errorf("error resetting file position: %w", err)
		}

		reader := csv.NewReader(config.file)

		// Skip header
		if _, err = reader.Read(); err != nil {
			return result, fmt.Errorf("error reading CSV header: %w", err)
		}

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return result, fmt.Errorf("error reading CSV: %w", err)
			}

			position, err := config.file.Seek(0, io.SeekCurrent)
			if err != nil {
				return result, fmt.Errorf("error getting file position: %w", err)
			}
			overallTracker.SetValue(overallPosition + position)

			i, err := strconv.ParseInt(record[0], 10, 32)
			if err != nil {
				fmt.Printf("Error parsing DocumentID %s: %v\n", record[0], err)
				continue
			}
			documentID := uint32(i)

			count, err := strconv.ParseInt(record[1], 10, 64)
			if err != nil {
				fmt.Printf("Error parsing count for DocumentID %d: %v\n", documentID, err)
				continue
			}

			result.totalDocuments++
			result.totalIDs += count

			messages := []string{
				fmt.Sprintf("Documents processed: %s\n", humanize.Comma(result.totalDocuments)),
				fmt.Sprintf("IDs created: %s\n", humanize.Comma(result.totalIDs)),
			}

			partitionIndex := hash(documentID) % uint32(config.partitions)
			partition := &partitionList[partitionIndex]

			if count > config.outlier && !loggedOutliers[documentID] {
				pinnedMessages = append(pinnedMessages, fmt.Sprintf("Skipping outlier DocumentID %d with count %s and partition %d\n", documentID, humanize.Comma(count), partitionIndex))
				loggedOutliers[documentID] = true
				continue
			}

			config.writer.SetPinnedMessages(append(messages, pinnedMessages...)...)

			if !partition.errored {
				newValue := partition.current + count
				if newValue > partition.max {
					partitionTrackers[partitionIndex].IncrementWithError(partition.max - partition.current)
					config.writer.Log(fmt.Sprintf("Partition %d exceeded maximum value\n", partitionIndex))
					partition.errored = true
					erroredPartitions++
					if erroredPartitions == config.partitions {
						break
					}
				} else {
					increment := newValue - partition.current
					partition.current = newValue
					partitionTrackers[partitionIndex].Increment(increment)
					result.partitionCounts[partitionIndex] = newValue
				}
			}
		}
	}

	position, err := config.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return result, fmt.Errorf("error getting file position: %w", err)
	}
	overallTracker.SetValue(overallPosition + position)

	messages := []string{
		fmt.Sprintf("Documents processed: %s\n", humanize.Comma(result.totalDocuments)),
		fmt.Sprintf("IDs created: %s\n", humanize.Comma(result.totalIDs)),
	}

	config.writer.SetPinnedMessages(append(messages, pinnedMessages...)...)

	if erroredPartitions == config.partitions {
		return result, fmt.Errorf("all partitions have exceeded their maximum value")
	} else if erroredPartitions == 1 {
		return result, fmt.Errorf("%d partition exceeded its maximum value", erroredPartitions)
	} else if erroredPartitions > 0 {
		return result, fmt.Errorf("%d partitions exceeded their maximum value", erroredPartitions)
	}
	return result, nil
}

// Based on Postgres hash_bytes_uint32 (https://doxygen.postgresql.org/hashfn_8c_source.html)
func hash(k uint32) uint32 {
	a := uint32(0x9e3779b9 + 4 + 3923095)
	b := a
	c := a

	a += k

	c ^= b
	c -= rotateLeft(b, 14)
	a ^= c
	a -= rotateLeft(c, 11)
	b ^= a
	b -= rotateLeft(a, 25)
	c ^= b
	c -= rotateLeft(b, 16)
	a ^= c
	a -= rotateLeft(c, 4)
	b ^= a
	b -= rotateLeft(a, 14)
	c ^= b
	c -= rotateLeft(b, 24)

	return c
}

func rotateLeft(x uint32, k uint) uint32 {
	return (x << k) | (x >> (32 - k))
}
