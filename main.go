package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strings"
)

type Clusters struct {
	Clusters []*Cluster `json: "clusters"`
}

type Cluster struct {
	Record         string `json: "record"`
	Subscription   string `json:"subscription"`
	ResourceGroup  string `json:"resourceGroup"`
	ManagedCluster string `json:"managedCluster"`
	Location       string `json:"location"`
}

var (
	fileName       = "cluster_classifications.csv"
	exportFileName = "cluster_classifications_cleaned.csv"
)

func main() {
	clusters := Clusters{}

	err := clusters.getRecords(fileName)
	if err != nil {
		fmt.Println("Unable to get records")
		os.Exit(1)
	}
	clusters.parseRecords()
	if len(clusters.Clusters) == 0 {
		fmt.Println("Unable to extract records, exiting")
		os.Exit(1)
	}
	file, err := clusters.createFile(exportFileName)
	if err != nil {
		os.Exit(1)
	}
	err = clusters.writeClustersToFile(file)
	if err != nil {
		fmt.Printf("Unable to write records to new file %v, exiting", file)
		os.Exit(1)
	}
	fmt.Printf("Created file %s and successfully wrote records to file", exportFileName)
}

func (c *Clusters) getRecords(file string) error {
	csvFile, _ := os.Open(fileName)
	reader := csv.NewReader(bufio.NewReader(csvFile))
	lines, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Cannot read file contents")
		return err
	}
	for _, line := range lines {
		c.Clusters = append(c.Clusters, &Cluster{
			Record: line[0],
		})
	}
	return nil
}

func (c *Clusters) parseRecords() {
	for _, cluster := range c.Clusters {
		details := strings.Split(cluster.Record, "/")
		if len(details) < 5 {
			fmt.Printf("Corrupted input %s skipping", cluster.Record)
			continue
		}
		cluster.Subscription = details[2]
		cluster.ResourceGroup = details[4]
		cluster.ManagedCluster = details[8]
	}
}

func (c *Clusters) createFile(fileName string) (*os.File, error) {
	file, err := os.Create(fileName)
	if err != nil || file == nil {
		return nil, errors.New("Unable to create file")
	}
	return file, nil
}

func (c *Clusters) writeClustersToFile(file *os.File) error {
	writer := csv.NewWriter(bufio.NewWriter(file))

	for _, cluster := range c.Clusters {
		err := writer.Write([]string{
			cluster.Subscription,
			cluster.ResourceGroup,
			cluster.ManagedCluster,
		})
		if err != nil {
			fmt.Printf("Unable to write record %v", cluster)
			return err
		}
	}
	return nil

}
