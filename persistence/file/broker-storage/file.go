package broker_storage

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type FileStorage interface {
	AppendToFile(string, map[int64]string) error
}
type fileStorage struct{}

func NewFileStorage() FileStorage {
	return &fileStorage{}
}

// AppendToFile
/*
	json format is not a good option to store data in file. because retrieve data from file and append in every two
	seconds is not performable. Hence, I store data in key-value style. tke key is messageID and the value is message.
	line are separated via "\n"
	we use serialize (turn map to string) and deserialize (string to map) data
*/
func (f *fileStorage) AppendToFile(fileName string, data map[int64]string) error {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0700)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for key, val := range data {
		line := fmt.Sprintf("%d %s\n", key, val)
		if _, err = file.WriteString(line); err != nil { // append the Line of data into file
			return err
		}
		delete(data, key)
	}

	return nil
}

func ReadFileContent(fileName string) (map[int64]string, error) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println(err)
		fmt.Println(err)
	}

	mapData := make(map[int64]string)
	dataArray := strings.Split(string(data), "\n")
	if len(dataArray) > 1 {
		for _, item := range dataArray {
			keyVal := strings.Split(item, " ")
			messageID, _ := strconv.ParseInt(keyVal[0], 10, 64)
			message := keyVal[1]
			mapData[messageID] = message
		}
	}

	return mapData, err
}
