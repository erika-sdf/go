package nosql

import (
	"github.com/gofiber/fiber/v2"
	"github.com/stellar/go/services/horizon/nosql/processors"
	"github.com/stellar/go/support/log"
	"strconv"
)

type TransactionHandler struct {
	db *BoltStore
}

func (h *TransactionHandler) Handler(c *fiber.Ctx) error {
	id := c.Params("id")
	txnId, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return err
	}
	//log.Errorf("get txn %d", txnId)
	ledgerId, err := h.db.GetLedgerFromTransaction(txnId)
	if err != nil {
		return err
	}
	lcm, err := h.db.GetLedger(ledgerId)
	if err != nil {
		return err
	}
	p , err := processors.NewTransactionProcessor(lcm)
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}
	l, err := p.GetTransaction()
	if err != nil {
		return err
	}
	return c.JSON(l)
}


func (h *TransactionHandler) ByLedgerHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	i, err := strconv.Atoi(id)
	if err != nil {
		return err
	}
	log.Errorf("get ledger %d", i)
	lcm, err := h.db.GetLedger(uint32(i))
	if err != nil {
		log.Error(err)
		return err
	}
	p , err := processors.NewTransactionProcessor(lcm)
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}
	limit := c.Query("limit")
	l, err := strconv.Atoi(limit)
	if err != nil {
		l = 10
	}

	t, err := p.GetTransactionsForLedger(l)
	if err != nil {
		log.Error(err)
		return err
	}
	return c.JSON(t)
}