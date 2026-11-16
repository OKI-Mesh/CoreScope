// Package dbconfig provides the shared DBConfig struct used by both the server
// and ingestor binaries for the Postgres connection and startup-load settings.
package dbconfig

import "os"

// DBConfig holds the database settings shared by the server and ingestor.
type DBConfig struct {
	ConnectionString string `json:"connectionString"` // Postgres DSN; $DATABASE_URL is the fallback

	// Load controls chunked startup loading (#1009).
	Load *LoadConfig `json:"load,omitempty"`
}

// LoadConfig controls the chunked startup-load behavior (#1009).
type LoadConfig struct {
	// ChunkSize is the number of transmission rows fetched per chunk
	// during PacketStore.LoadChunked. 0/unset → 10000.
	ChunkSize int `json:"chunkSize"`
}

// DSN returns the Postgres connection string: connectionString from config.json,
// else $DATABASE_URL. It is empty when neither is set. It is safe to call on a
// nil *DBConfig, which is what a config.json without a "db" block produces.
func (c *DBConfig) DSN() string {
	if c != nil && c.ConnectionString != "" {
		return c.ConnectionString
	}
	return os.Getenv("DATABASE_URL")
}

// GetLoadChunkSize returns the configured chunk size or 10000 default (#1009).
func (c *DBConfig) GetLoadChunkSize() int {
	if c != nil && c.Load != nil && c.Load.ChunkSize > 0 {
		return c.Load.ChunkSize
	}
	return 10000
}
