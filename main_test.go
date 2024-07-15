package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvenDistribution(t *testing.T) {
	numRecords := 100000
	numPartitions := 10

	tmpfile := createTestCSV(t, numRecords, 1)
	defer os.Remove(tmpfile.Name())

	result, _, err := runProcessCSV(t, tmpfile, Config{partitions: numPartitions})
	require.NoError(t, err)

	assert.EqualValues(t, numRecords, result.totalDocuments)
	assert.EqualValues(t, numRecords, result.totalIDs)

	expectedCount := numRecords / numPartitions
	deviation := 0.05

	lowerBound := int64(float64(expectedCount) * (1 - deviation))
	upperBound := int64(float64(expectedCount) * (1 + deviation))

	for i, count := range result.partitionCounts {
		assert.GreaterOrEqual(t, count, lowerBound, "Partition %d has too few records: %d vs %d", i, count, lowerBound)
		assert.LessOrEqual(t, count, upperBound, "Partition %d has too many records: %d vs %d", i, count, upperBound)
	}
}

func TestAllPartitionsErrored(t *testing.T) {
	tmpfile := createTestCSV(t, 10, 11)
	defer os.Remove(tmpfile.Name())

	result, output, err := runProcessCSV(t, tmpfile, Config{partitions: 3, max: 10})

	require.Error(t, err)
	assert.EqualValues(t, result.totalDocuments, 3)
	assert.EqualValues(t, result.totalIDs, 33)

	expectedError := "all partitions have exceeded their maximum value"
	assert.Equal(t, expectedError, err.Error(), "Unexpected error message")

	assert.Contains(t, output, "Partition 000", "Expected progress bar for partition 3")
	assert.Contains(t, output, "Partition 001", "Expected progress bar for partition 1")
	assert.Contains(t, output, "Partition 002", "Expected progress bar for partition 2")
	assert.Contains(t, output, "Total Progress", "Expected overall progress bar")
}

func createTestCSV(t *testing.T, numDocuments int, count int) *os.File {
	var csvData bytes.Buffer
	csvData.WriteString("DocumentID,count\n")
	for i := 1; i <= numDocuments; i++ {
		csvData.WriteString(fmt.Sprintf("%d,%d\n", i, count))
	}

	tmpfile, err := ioutil.TempFile("", "test.csv")
	require.NoError(t, err)

	_, err = tmpfile.Write(csvData.Bytes())
	require.NoError(t, err)
	_, err = tmpfile.Seek(0, 0)
	require.NoError(t, err)

	return tmpfile
}

// FIXME The postgres implementation returns signed values
//func TestHash(t *testing.T) {
//	testCases := []struct {
//		input    uint32
//		expected int32
//	}{
//		{input: 123456789, expected: 524883300},
//		{input: 987654321, expected: -522295545},
//		{input: 1, expected: -1905060026},
//		{input: 2147483647, expected: -96758253}, // 2^31 - 1
//		{input: 42, expected: 1509752520},
//		{input: 314159265, expected: -489000246},
//		{input: 271828182, expected: -562529542},
//		{input: 1618033988, expected: -1731020505},
//		{input: 272321, expected: 235424784},
//	}
//
//	for _, tc := range testCases {
//		t.Run(fmt.Sprintf("input_%d", tc.input), func(t *testing.T) {
//			result := hash(tc.input)
//			assert.Equal(t, tc.expected, result, "Hash result doesn't match expected value for input %d", tc.input)
//		})
//	}
//}

func runProcessCSV(t *testing.T, file *os.File, userConfig Config) (Result, string, error) {
	pw := progress.NewWriter()
	pw.SetAutoStop(true)
	pw.SetUpdateFrequency(time.Millisecond)

	var buf bytes.Buffer
	pw.SetOutputWriter(&buf)

	fileInfo, err := file.Stat()
	require.NoError(t, err)

	defaultConfig := Config{
		iterations: 1,
		partitions: 1,
		max:        math.MaxInt64,
		min:        0,
		file:       file,
		writer:     pw,
		fileSize:   fileInfo.Size(),
		outlier:    math.MaxInt64,
	}

	config := mergeConfig(defaultConfig, userConfig)

	go pw.Render()

	for !config.writer.IsRenderInProgress() {
		time.Sleep(10 * time.Millisecond)
	}

	result, err := ProcessCSV(config)

	pw.Stop()

	for pw.IsRenderInProgress() {
		time.Sleep(100 * time.Millisecond)
	}

	return result, buf.String(), err
}

func mergeConfig(defaultConfig Config, userConfig Config) Config {
	mergedConfig := defaultConfig

	if userConfig.iterations != 0 {
		mergedConfig.iterations = userConfig.iterations
	}
	if userConfig.partitions != 0 {
		mergedConfig.partitions = userConfig.partitions
	}
	if userConfig.max != 0 {
		mergedConfig.max = userConfig.max
	}
	if userConfig.min != 0 {
		mergedConfig.min = userConfig.min
	}
	if userConfig.outlier != 0 {
		mergedConfig.outlier = userConfig.outlier
	}
	if userConfig.file != nil {
		mergedConfig.file = userConfig.file
	}
	if userConfig.writer != nil {
		mergedConfig.writer = userConfig.writer
	}
	if userConfig.fileSize != 0 {
		mergedConfig.fileSize = userConfig.fileSize
	}

	return mergedConfig
}
