package server

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	defaultDir        = "/tmp"
	defaultDBFilename = "dump.rdb"
	defaultPort       = "6379"
	defaultReplicaOf  = ""
)

type Config struct {
	Dir        string
	DBFilename string
	Port       string
	ReplicaOf  string
}

func (cfg *Config) Get(key string) (string, bool) {
	switch key {
	case "dir":
		return cfg.Dir, true
	case "dbfilename":
		return cfg.DBFilename, true
	case "port":
		return cfg.Port, true
	default:
		return "", false
	}
}
func (cfg *Config) GetInt(key string) (int, bool) {
	switch key {
	case "port":
		port, _ := strconv.Atoi(cfg.Port)
		return port, true
	default:
		return 0, false
	}
}

func (cfg *Config) Validate() error {
	// Validate directory exists and is writable
	if info, err := os.Stat(cfg.Dir); err != nil {
		return fmt.Errorf("invalid dir: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("dir %s is not a directory", cfg.Dir)
	}

	// Validate port number
	if port, err := strconv.Atoi(cfg.Port); err != nil {
		return fmt.Errorf("invalid port number: %w", err)
	} else if port < 1 || port > 65535 {
		return fmt.Errorf("port number %d out of range (1-65535)", port)
	}

	// Validate dbfilename
	if cfg.DBFilename == "" {
		return fmt.Errorf("dbfilename cannot be empty")
	}

	// Validate replicaof format if specified
	if cfg.ReplicaOf != "" {
		parts := strings.Split(cfg.ReplicaOf, " ")
		if len(parts) != 2 {
			return fmt.Errorf("replicaof must be in format 'host port'")
		}
		if port, err := strconv.Atoi(parts[1]); err != nil {
			return fmt.Errorf("invalid replica port number: %w", err)
		} else if port < 1 || port > 65535 {
			return fmt.Errorf("replica port number %d out of range (1-65535)", port)
		}
	}

	return nil
}

func ReadConfig() Config {
	// Receive, parse and validate the configuration
	dir := flag.String("dir", defaultDir, "Directory for database files")
	dbfilename := flag.String("dbfilename", defaultDBFilename, "Database filename")
	port := flag.String("port", defaultPort, "Port to listen on")
	replicaof := flag.String("replicaof", defaultReplicaOf, "Replica of")

	flag.Parse()

	config := Config{
		Dir:        *dir,
		DBFilename: *dbfilename,
		Port:       *port,
		ReplicaOf:  *replicaof,
	}

	if err := config.Validate(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	return config
}
