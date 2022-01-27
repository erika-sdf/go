package nosql
//
//import (
//	"github.com/gofiber/fiber/v2"
//	"github.com/stellar/go/services/horizon/nosql/processors"
//	"github.com/stellar/go/support/log"
//	"strconv"
//)
//
//type TransactionHandler struct {
//	db *BoltStore
//}
//
//func (h *TransactionHandler) Handler(c *fiber.Ctx) error {
//	id := c.Params("id")
//	opId, err := strconv.Atoi(id)
//	if err != nil {
//		return err
//	}
//	log.Errorf("get op %d", opId)
//	txnId, err := h.db.GetOpFromTxn(opId)
//	if err != nil {
//		return err
//	}
//	ledgerId, err := h.db.GetLedgerFromTxn(txnId)
//	if err != nil {
//		return err
//	}
//	lcm, err := h.db.GetLedger(ledgerId)
//	if err != nil {
//		return err
//	}
//	p , err := processors.NewOperationProcessor(lcm)
//	if err != nil {
//		log.Errorf("err %v", err)
//		return err
//	}
//	l, err := p.GetOperation()
//	if err != nil {
//		return err
//	}
//	return c.JSON(l)
//}
