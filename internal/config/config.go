package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	ClickhouseCFG   ClickhouseCFG
	OrchestratorCFG OrchestratorCFG
}

type OrchestratorCFG struct {
	RAW_SPANS_LIMIT int
}

type ClickhouseCFG struct {
	URL                      string
	DB                       string
	USERNAME                 string
	PASSWD                   string
	MAX_EXEC_SEC             int
	COMPRESSION_METHOD       string
	DIAL_TIMEOUT_SEC         int
	MAX_OPEN_CONNS           int
	MAX_IDLE_CONNS           int
	CONN_MAX_LIFESPAN_MINUTE int
	CONN_OPEN_STRATEGY       string
	BLOCK_BUFFER_SIZE        int
	MAX_COMPRESSION_BUFFER   int
	CLIENT_NAME              string
	CLIENT_VERSION           string
	RAW_SPAN_TABLE_NAME      string
}

func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println(".env file could not be found")
	}
	chCFG := &ClickhouseCFG{
		URL:                      getEnv("CLICKHOUSE_URL", "http://localhost:8123"),
		DB:                       getEnv("DB_NAME", "telemetry"),
		USERNAME:                 getEnv("USERNAME", "admin"),
		PASSWD:                   getEnv("PASSWD", "1234"),
		MAX_EXEC_SEC:             getEnvAsInt("MAX_EXEC_SEC", 60),
		COMPRESSION_METHOD:       getEnv("COMPRESSION_METHOD", "LZ4"),
		DIAL_TIMEOUT_SEC:         getEnvAsInt("DIAL_TIMEOUT_SEC", 30),
		MAX_OPEN_CONNS:           getEnvAsInt("MAX_OPEN_CONNS", 5),
		MAX_IDLE_CONNS:           getEnvAsInt("MAX_IDLE_CONNS", 5),
		CONN_MAX_LIFESPAN_MINUTE: getEnvAsInt("CONN_MAX_LIFESPAN_MINUTE", 10),
		CONN_OPEN_STRATEGY:       getEnv("CONN_OPEN_STRATEGY", "OpenInOrder"),
		BLOCK_BUFFER_SIZE:        getEnvAsInt("BLOCK_BUFFER_SIZE", 10),
		MAX_COMPRESSION_BUFFER:   getEnvAsInt("MAX_COMPRESSION_BUFFER", 10240),
		CLIENT_NAME:              getEnv("CLIENT_NAME", "Orchestrator-GO"),
		CLIENT_VERSION:           getEnv("CLIENT_VERSION", "N/A"),
		RAW_SPAN_TABLE_NAME:      getEnv("RAW_SPAN_TABLE_NAME", "spans_raw"),
	}
	orCFG := &OrchestratorCFG{
		RAW_SPANS_LIMIT: getEnvAsInt("RAW_SPANS_LIMIT", 10),
	}
	return &Config{
		ClickhouseCFG:   *chCFG,
		OrchestratorCFG: *orCFG,
	}
}

func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func getEnvAsInt(name string, defaultVal int) int {
	valStr := getEnv(name, "")
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	log.Printf("Warning: could not parse %s=%s as int, using default %d", name, valStr, defaultVal)
	return defaultVal
}
