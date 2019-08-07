package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	showStreamsCmd string = "{\"ksql\": \"SHOW STREAMS EXTENDED;\"}"
	showTablesCmd  string = "{\"ksql\": \"SHOW TABLES EXTENDED;\"}"
	contentType    string = "application/vnd.ksql.v1+json; charset=utf-8"
)

type query struct {
	QueryString string `json:"queryString"`
}

type description struct {
	Name         string  `json:"name"`
	ReadQueries  []query `json:"readQueries"`
	WriteQueries []query `json:"writeQueries"`
}

type statement struct {
	Descriptions []description `json:"sourceDescriptions"`
}

func main() {

	args := os.Args
	if len(args) != 5 {
		fmt.Println("Not enough arguments!")
		os.Exit(0)
	}
	ksqlServer := args[2]
	fileName := args[4]
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Flush Streams
	file.WriteString("/*************************************/\n")
	file.WriteString("/*              Streams              */\n")
	file.WriteString("/*************************************/\n")
	file.WriteString("\n")

	payload := strings.NewReader(showStreamsCmd)
	req, err := http.NewRequest("POST", ksqlServer, payload)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	stmts := make([]statement, 0)
	err = json.Unmarshal(respBytes, &stmts)
	if err != nil {
		panic(err)
	}

	for _, stmt := range stmts {
		for _, desc := range stmt.Descriptions {
			file.WriteString(fmt.Sprintf("/*** %s ***/ \n", desc.Name))
			file.WriteString(fmt.Sprintf("%s \n", desc.ReadQueries[0].QueryString))
			file.WriteString("\n")
		}
	}
	file.Sync()

	// Flush Tables
	file.WriteString("/*************************************/\n")
	file.WriteString("/*              Tables               */\n")
	file.WriteString("/*************************************/\n")
	file.WriteString("\n")

	payload = strings.NewReader(showTablesCmd)
	req, err = http.NewRequest("POST", ksqlServer, payload)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err = httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	respBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	stmts = make([]statement, 0)
	err = json.Unmarshal(respBytes, &stmts)
	if err != nil {
		panic(err)
	}

	for _, stmt := range stmts {
		for _, desc := range stmt.Descriptions {
			file.WriteString(fmt.Sprintf("/*** %s ***/ \n", desc.Name))
			file.WriteString(fmt.Sprintf("%s \n", desc.WriteQueries[0].QueryString))
			file.WriteString("\n")
		}
	}
	file.Sync()
	fmt.Println("File created successfully!")

}
