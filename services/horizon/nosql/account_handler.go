package nosql

import (
	"github.com/gofiber/fiber/v2"
	"github.com/stellar/go/services/horizon/nosql/processors"
	"github.com/stellar/go/support/log"
)

type AccountHandler struct {
	db *BoltStore
}

func (h *AccountHandler) Handler(c *fiber.Ctx) error {
	id := c.Params("id")
	log.Errorf("get account %d", id)
	aChg, err := h.db.GetAccount(id)
	if err != nil {
		log.Error(err)
		return err
	}
	p , err := processors.NewAccountProcessor(aChg)
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}
	a, err := p.GetAccount()
	if err != nil {
		return err
	}
	return c.JSON(a)
}