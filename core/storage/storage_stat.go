package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/akrylysov/pogreb"
	"time"
	"tinymq/config"
)

type storageStat struct {
	size       int64
	minValue   int64
	maxValue   int64
	rangeWidth float32
	histogram  []int
}

func (stat *storageStat) collectStat(storage config.MessageStorageInterface) (*storageStat, error) {
	minValue := stat.minValue

	result := new(storageStat)

	const histogramSize = 1000
	result = &storageStat{
		size:      0,
		minValue:  0,
		maxValue:  time.Now().UnixNano(),
		histogram: make([]int, histogramSize),
	}

	if minValue == 0 {
		items := storage.Items()
		for {
			_, value, err := stat.nextKeyValue(storage, items)
			if errors.Is(err, pogreb.ErrIterationDone) {
				break
			}

			if err != nil {
				return nil, err
			}

			if minValue == 0 {
				minValue = value
			}

			if value < minValue {
				minValue = value
			}
		}

		result.minValue = minValue
	}

	result.rangeWidth = float32(result.maxValue-result.minValue) / float32(histogramSize)

	items := storage.Items()
	for {
		_, value, err := stat.nextKeyValue(storage, items)
		if errors.Is(err, pogreb.ErrIterationDone) {
			break
		}

		if err != nil {
			return nil, err
		}

		result.incrementStat(value, 1)
	}

	return result, nil
}

func (stat *storageStat) GC(storage config.MessageStorageInterface, maxItems int64) error {
	if stat.minValue == 0 {
		return nil
	}

	if stat.size == 0 {
		return nil
	}

	if stat.size < maxItems {
		return nil
	}

	itemsToDelete := stat.size - maxItems

	if itemsToDelete <= 0 {
		return nil
	}

	var target float32

	for i, count := range stat.histogram {
		if itemsToDelete > int64(count) {
			itemsToDelete -= int64(count)
			continue
		}

		target = float32(i) + (float32(itemsToDelete) / float32(count))

		break
	}

	rangeWidth := float32(stat.maxValue-stat.minValue) / float32(len(stat.histogram))

	targetTime := stat.minValue + int64(rangeWidth*target)

	items := storage.Items()
	hasWrites := false

	itemsToDelete = stat.size - maxItems

	for {
		key, value, err := stat.nextKeyValue(storage, items)
		if errors.Is(err, pogreb.ErrIterationDone) {
			break
		}

		if err != nil {
			return err
		}

		if value < targetTime {
			err = storage.Delete(key)
			if err != nil {
				return err
			}

			stat.decrementStat(value, 1)
			itemsToDelete--

			if itemsToDelete <= 0 {
				break
			}

			hasWrites = true
		}
	}

	if hasWrites {
		err := storage.Sync()
		if err != nil {
			return err
		}
	}

	return nil
}

func (stat *storageStat) incrementStat(value int64, n int) {
	if stat.minValue == 0 {
		return
	}

	valueRangeId := int(float32(value-stat.minValue) / stat.rangeWidth)

	if valueRangeId > len(stat.histogram) {
		return
	}

	stat.histogram[valueRangeId] += n
	stat.size++
}

func (stat *storageStat) decrementStat(value int64, n int) {
	if stat.minValue == 0 {
		return
	}
	valueRangeId := int(float32(value-stat.minValue) / stat.rangeWidth)
	if stat.histogram[valueRangeId] < n {
		stat.histogram[valueRangeId] = 0
	} else {
		stat.histogram[valueRangeId] -= n
	}
	stat.size--
}

func (stat *storageStat) nextKeyValue(storage config.MessageStorageInterface, items config.ItemsIterator) ([]byte, int64, error) {
	keyBytes, valueBytes, err := storage.Next(items)

	if err != nil {
		return nil, 0, err
	}

	var value int64
	err = binary.Read(bytes.NewReader(valueBytes), binary.LittleEndian, &value)

	if err != nil {
		return nil, 0, err
	}

	return keyBytes, value, nil
}
