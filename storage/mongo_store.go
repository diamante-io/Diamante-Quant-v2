package storage

import (
	"context"
	"fmt"
	"time"

	"diamante/common"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoStore implements the Store interface using MongoDB.
type MongoStore struct {
	client     *mongo.Client
	db         *mongo.Database
	blocksColl *mongo.Collection
}

// NewMongoStore creates a new MongoStore instance.
func NewMongoStore(connectionString, dbName string) (*MongoStore, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(connectionString)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %w", err)
	}

	db := client.Database(dbName)
	ms := &MongoStore{
		client:     client,
		db:         db,
		blocksColl: db.Collection("blocks"),
	}
	return ms, nil
}

// SaveBlock stores a block document.
func (ms *MongoStore) SaveBlock(block *common.Block) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := ms.blocksColl.InsertOne(ctx, block)
	return err
}

// GetBlock retrieves a block by its number.
func (ms *MongoStore) GetBlock(blockNumber uint64) (*common.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"number": blockNumber}
	var block common.Block
	err := ms.blocksColl.FindOne(ctx, filter).Decode(&block)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve block %d: %w", blockNumber, err)
	}
	return &block, nil
}
