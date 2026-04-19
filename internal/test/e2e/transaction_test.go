package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestE2E_MultiTableTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := Setup(t)
	defer env.Teardown(ctx)

	// Create related tables
	_, err := env.Postgres.Exec(`
		CREATE TABLE orders (
			order_id SERIAL PRIMARY KEY,
			customer_name TEXT,
			order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE order_items (
			item_id SERIAL PRIMARY KEY,
			order_id INT REFERENCES orders(order_id),
			product_name TEXT,
			quantity INT
		);
	`)
	require.NoError(t, err)

	// Configure and Start pipeline
	pCfg := env.GetDefaultPipelineConfig("p_tx")
	pCfg.Tables = []string{"orders", "order_items"}
	err = env.SetPipelineConfig("p_tx", pCfg)
	require.NoError(t, err)

	env.StartWorker()
	env.EventuallyAssertHeartbeat("p_tx", "Running", 30*time.Second)

	// Test Case 1: Atomic Multi-Table Commit
	t.Log("Executing multi-table committed transaction...")
	tx, err := env.Postgres.BeginTx(ctx, nil)
	require.NoError(t, err)

	var orderID1 int
	err = tx.QueryRowContext(ctx, "INSERT INTO orders (customer_name) VALUES ($1) RETURNING order_id", "Alice").Scan(&orderID1)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, "INSERT INTO order_items (order_id, product_name, quantity) VALUES ($1, $2, $3)", orderID1, "Laptop", 1)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO order_items (order_id, product_name, quantity) VALUES ($1, $2, $3)", orderID1, "Mouse", 2)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify both tables in Databend
	require.Eventually(t, func() bool {
		orderCount, _ := env.GetDatabendRowCount("orders")
		itemCount, _ := env.GetDatabendRowCount("order_items")
		return orderCount == 1 && itemCount == 2
	}, 30*time.Second, 1*time.Second, "Committed multi-table transaction should be synced")

	// Test Case 2: Rollback Isolation
	t.Log("Executing multi-table rolled-back transaction...")
	tx, err = env.Postgres.BeginTx(ctx, nil)
	require.NoError(t, err)

	var orderID2 int
	err = tx.QueryRowContext(ctx, "INSERT INTO orders (customer_name) VALUES ($1) RETURNING order_id", "Bob").Scan(&orderID2)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, "INSERT INTO order_items (order_id, product_name, quantity) VALUES ($1, $2, $3)", orderID2, "Monitor", 1)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)

	// Verify no new data in Databend after rollback
	time.Sleep(5 * time.Second)
	orderCount, _ := env.GetDatabendRowCount("orders")
	itemCount, _ := env.GetDatabendRowCount("order_items")
	require.Equal(t, uint64(1), orderCount, "Rolled back order should NOT be synced")
	require.Equal(t, uint64(2), itemCount, "Rolled back items should NOT be synced")

	// Test Case 3: Mixed Operations in one TX
	t.Log("Executing mixed operations transaction...")
	tx, err = env.Postgres.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, "UPDATE orders SET customer_name = $1 WHERE customer_name = $2", "Alice Smith", "Alice")
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, "DELETE FROM order_items WHERE product_name = $1", "Mouse")
	require.NoError(t, err)

	// Use orderID1 which is definitely valid
	_, err = tx.ExecContext(ctx, "INSERT INTO order_items (order_id, product_name, quantity) VALUES ($1, $2, $3)", orderID1, "Keyboard", 1)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify state in Databend
	require.Eventually(t, func() bool {
		var name string
		err := env.Databend.QueryRow("SELECT customer_name FROM orders WHERE customer_name = 'Alice Smith'").Scan(&name)
		if err != nil { return false }
		
		var count uint64
		err = env.Databend.QueryRow("SELECT count(*) FROM order_items").Scan(&count)
		if err != nil { return false }
		
		return name == "Alice Smith" && count == 2
	}, 30*time.Second, 1*time.Second, "Mixed operations transaction should be eventually consistent")

	t.Log("Successfully verified multi-table transactional integrity")
}
