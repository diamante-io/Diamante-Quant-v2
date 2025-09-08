package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"diamante/common"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoLedger implements the common.LedgerAPI interface using MongoDB.
type MongoLedger struct {
	client           *mongo.Client
	db               *mongo.Database
	accountsColl     *mongo.Collection
	blocksColl       *mongo.Collection
	transactionsColl *mongo.Collection
	snapshotsColl    *mongo.Collection
	contractsColl    *mongo.Collection
	currentHeight    int
	mu               sync.RWMutex
}

// NewMongoLedger creates a new MongoLedger instance given a connection string and database name.
func NewMongoLedger(connectionString, dbName string) (*MongoLedger, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(connectionString)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %w", err)
	}

	db := client.Database(dbName)
	ml := &MongoLedger{
		client:           client,
		db:               db,
		accountsColl:     db.Collection("accounts"),
		blocksColl:       db.Collection("blocks"),
		transactionsColl: db.Collection("transactions"),
		snapshotsColl:    db.Collection("snapshots"),
		contractsColl:    db.Collection("contracts"),
	}

	if err := ml.createIndexes(ctx); err != nil {
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}

	height, err := ml.getCurrentHeightFromDB(ctx)
	if err != nil {
		return nil, err
	}
	ml.currentHeight = height

	return ml, nil
}

func (ml *MongoLedger) createIndexes(ctx context.Context) error {
	_, err := ml.blocksColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "blockHeight", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "hash", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create blocks indexes: %w", err)
	}

	_, err = ml.transactionsColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "txHash", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{Keys: bson.D{{Key: "from", Value: 1}}},
		{Keys: bson.D{{Key: "to", Value: 1}}},
		{Keys: bson.D{{Key: "blockHeight", Value: 1}}},
	})
	if err != nil {
		return fmt.Errorf("failed to create transactions indexes: %w", err)
	}

	_, err = ml.accountsColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "address", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create accounts index: %w", err)
	}

	_, err = ml.snapshotsColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "blockHeight", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create snapshots index: %w", err)
	}

	_, err = ml.contractsColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "contractID", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create contracts index: %w", err)
	}

	return nil
}

func (ml *MongoLedger) getCurrentHeightFromDB(ctx context.Context) (int, error) {
	opts := options.FindOne().SetSort(bson.D{{Key: "blockHeight", Value: -1}})
	var result bson.M
	err := ml.blocksColl.FindOne(ctx, bson.D{}, opts).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, err
	}
	switch v := result["blockHeight"].(type) {
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	default:
		return 0, errors.New("unable to parse blockHeight")
	}
}

// CreateAccount inserts a new account document.
func (ml *MongoLedger) CreateAccount(ac *common.Account) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var existing common.Account
	err := ml.accountsColl.FindOne(ctx, bson.M{"address": ac.ID}).Decode(&existing)
	if err == nil {
		return fmt.Errorf("account %s already exists", ac.ID)
	} else if err != mongo.ErrNoDocuments {
		return err
	}

	_, err = ml.accountsColl.InsertOne(ctx, bson.M{
		"address":   ac.ID,
		"publicKey": ac.PublicKey,
		"balance":   ac.Balance,
		"nonce":     0,
	})
	return err
}

// UpdateAccount updates an account document.
func (ml *MongoLedger) UpdateAccount(ac *common.Account) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"address": ac.ID}
	update := bson.M{"$set": bson.M{
		"publicKey": ac.PublicKey,
		"balance":   ac.Balance,
		"nonce":     ac.Nonce,
	}}
	_, err := ml.accountsColl.UpdateOne(ctx, filter, update)
	return err
}

// GetBalance returns the balance of the account.
func (ml *MongoLedger) GetBalance(accountID string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var ac common.Account
	err := ml.accountsColl.FindOne(ctx, bson.M{"address": accountID}).Decode(&ac)
	if err != nil {
		return 0, err
	}
	return ac.Balance, nil
}

// UpdateAccountBalance atomically increments the account balance.
func (ml *MongoLedger) UpdateAccountBalance(accountID string, amount float64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"address": accountID}
	update := bson.M{"$inc": bson.M{"balance": amount}}
	res, err := ml.accountsColl.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("account %s does not exist", accountID)
	}
	return nil
}

// AddTransaction validates and inserts a transaction document.
func (ml *MongoLedger) AddTransaction(tx common.Transaction) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var existing common.Transaction
	err := ml.transactionsColl.FindOne(ctx, bson.M{"txHash": tx.ID}).Decode(&existing)
	if err == nil {
		return fmt.Errorf("transaction %s already exists", tx.ID)
	} else if err != mongo.ErrNoDocuments {
		return err
	}

	_, err = ml.transactionsColl.InsertOne(ctx, bson.M{
		"txHash":      tx.ID,
		"from":        tx.Sender,
		"to":          tx.Receiver,
		"amount":      tx.Amount,
		"timestamp":   time.Unix(tx.Timestamp, 0),
		"signature":   tx.Signature,
		"nonce":       tx.Nonce,
		"blockHeight": tx.BlockHeight,
		"status":      "pending",
	})
	return err
}

// IsTransactionCommitted checks for the existence of a transaction.
func (ml *MongoLedger) IsTransactionCommitted(txID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var tx common.Transaction
	err := ml.transactionsColl.FindOne(ctx, bson.M{"txHash": txID}).Decode(&tx)
	return err == nil
}

// CommitBlock inserts a block and updates the status of its transactions atomically.
func (ml *MongoLedger) CommitBlock(block common.Block) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	session, err := ml.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return err
		}
		_, err := ml.blocksColl.InsertOne(sc, bson.M{
			"blockHeight":  block.Number,
			"hash":         block.Hash,
			"previousHash": block.PreviousHash,
			"timestamp":    block.Timestamp,
			"transactions": block.Transactions,
		})
		if err != nil {
			session.AbortTransaction(sc)
			return err
		}
		for _, tx := range block.Transactions {
			filter := bson.M{"txHash": tx.ID}
			update := bson.M{"$set": bson.M{"blockHeight": block.Number, "status": "committed"}}
			if _, err := ml.transactionsColl.UpdateOne(sc, filter, update); err != nil {
				session.AbortTransaction(sc)
				return err
			}
		}
		ml.mu.Lock()
		if block.Number > ml.currentHeight {
			ml.currentHeight = block.Number
		}
		ml.mu.Unlock()
		return session.CommitTransaction(sc)
	})
	return err
}

// GetBlockByNumber retrieves a block by its block height.
func (ml *MongoLedger) GetBlockByNumber(num int) (common.Block, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var block common.Block
	err := ml.blocksColl.FindOne(ctx, bson.M{"blockHeight": num}).Decode(&block)
	if err != nil {
		return common.Block{}, false
	}
	return block, true
}

// CreateSnapshot generates a snapshot of all account states at the given block height.
func (ml *MongoLedger) CreateSnapshot(height int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cursor, err := ml.accountsColl.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	stateData := make(map[string]interface{})
	for cursor.Next(ctx) {
		var ac bson.M
		if err := cursor.Decode(&ac); err != nil {
			return err
		}
		addr, _ := ac["address"].(string)
		stateData[addr] = ac
	}
	// Remove unused JSON marshalling; computeStateRoot does its own marshalling.
	snapshot := bson.M{
		"blockHeight": height,
		"stateRoot":   computeStateRoot(stateData),
		"stateData":   stateData,
		"timestamp":   time.Now(),
	}
	_, err = ml.snapshotsColl.InsertOne(ctx, snapshot)
	return err
}

// RestoreSnapshot resets account state to that captured in the snapshot.
func (ml *MongoLedger) RestoreSnapshot(height int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var snapshot bson.M
	err := ml.snapshotsColl.FindOne(ctx, bson.M{"blockHeight": height}).Decode(&snapshot)
	if err != nil {
		return fmt.Errorf("snapshot for height %d not found: %w", height, err)
	}
	stateData, ok := snapshot["stateData"].(map[string]interface{})
	if !ok {
		return errors.New("invalid stateData in snapshot")
	}

	var models []mongo.WriteModel
	for accountID, data := range stateData {
		models = append(models, mongo.NewReplaceOneModel().
			SetFilter(bson.M{"address": accountID}).
			SetReplacement(data).
			SetUpsert(true))
	}
	if len(models) > 0 {
		_, err = ml.accountsColl.BulkWrite(ctx, models)
		if err != nil {
			return fmt.Errorf("failed to restore accounts from snapshot: %w", err)
		}
	}
	ml.mu.Lock()
	ml.currentHeight = height
	ml.mu.Unlock()
	return nil
}

// IntegrityCheck verifies that there is at least one block in the ledger.
func (ml *MongoLedger) IntegrityCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	count, err := ml.blocksColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		return err
	}
	if count == 0 {
		return errors.New("no blocks found in ledger")
	}
	log.Println("MongoLedger: Integrity check passed")
	return nil
}

// --- Smart Contract Methods ---
// For production, we implement basic smart contract methods.

func (ml *MongoLedger) DeploySmartContract(sc *common.SmartContract) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc := bson.M{
		"contractID": sc.ID,
		"code":       sc.Code,
		"owner":      sc.Owner,
		"state":      sc.State,
		"ABI":        sc.ABI,
		"language":   sc.Language,
		"events":     sc.Events,
		"gasUsage":   sc.GasUsage,
	}
	_, err := ml.contractsColl.InsertOne(ctx, doc)
	return err
}

func (ml *MongoLedger) ExecuteSmartContract(scID, function, sender string, params map[string]interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var contract bson.M
	err := ml.contractsColl.FindOne(ctx, bson.M{"contractID": scID}).Decode(&contract)
	if err != nil {
		return nil, fmt.Errorf("contract %s not found", scID)
	}
	log.Printf("Executing smart contract %s, function %s by %s with params %v", scID, function, sender, params)
	// For production, integrate with your smart contract engine.
	// Here we simulate execution.
	return fmt.Sprintf("Executed function %s with params %v", function, params), nil
}

func (ml *MongoLedger) RemoveSmartContract(contractID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := ml.contractsColl.DeleteOne(ctx, bson.M{"contractID": contractID})
	return err
}

func computeStateRoot(stateData map[string]interface{}) string {
	data, err := json.Marshal(stateData)
	if err != nil {
		return ""
	}
	return common.HashData(data)
}
