package nosql

import (
	"github.com/gofiber/fiber/v2"
	"github.com/stellar/go/services/horizon/nosql/processors"
	"github.com/stellar/go/support/log"
	"strconv"
)

type LedgerHandler struct {
	db *BoltStore
}

func (h *LedgerHandler) Handler(c *fiber.Ctx) error {
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
	p , err := processors.NewLedgerProcessor(lcm)
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}
	l, err := p.GetLedger()
	if err != nil {
		return err
	}
	return c.JSON(l)
}

