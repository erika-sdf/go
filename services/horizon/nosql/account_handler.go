package nosql

import (
	"github.com/gofiber/fiber/v2"
)

type AccountHandler struct {
	db *BoltStore
}

type accountResponse struct {
	ID         string                    `json:"id"`
	Sequence   string                    `json:"sequence"`
	Thresholds accountResponseThresholds `json:"thresholds"`
	Signers    []accountResponseSigner   `json:"signers"`
	Balances   []accountResponseBalance  `json:"balances"`
}

type accountResponseThresholds struct {
	Low  uint8 `json:"low_threshold"`
	Med  uint8 `json:"med_threshold"`
	High uint8 `json:"high_threshold"`
}

type accountResponseSigner struct {
	Key    string `json:"key"`
	Weight uint32 `json:"weight"`
}

type accountResponseBalance struct {
	Asset   string `json:"asset"`
	Balance string `json:"balance"`
	Limit   string `json:"limit,omitempty"`
}

func (h *AccountHandler) Handler(c *fiber.Ctx) error {
	id := c.Params("id")
/*
	a, ok := h.Data.accounts[id]
	if !ok {
		return fiber.ErrNotFound
	}
	resp := accountResponse{
		ID:       a.AccountEntry.AccountId.Address(),
		Sequence: strconv.FormatInt(int64(a.AccountEntry.SeqNum), 10),
		Thresholds: accountResponseThresholds{
			Low:  a.AccountEntry.Thresholds.ThresholdLow(),
			Med:  a.AccountEntry.Thresholds.ThresholdMedium(),
			High: a.AccountEntry.Thresholds.ThresholdHigh(),
		},
		Signers: func() []accountResponseSigner {
			signers := []accountResponseSigner{
				{Key: a.AccountEntry.AccountId.Address(), Weight: uint32(a.AccountEntry.Thresholds.MasterKeyWeight())},
			}
			for _, s := range a.AccountEntry.Signers {
				signers = append(signers, accountResponseSigner{
					Key:    s.Key.Address(),
					Weight: uint32(s.Weight),
				})
			}
			return signers
		}(),
		Balances: func() []accountResponseBalance {
			balances := []accountResponseBalance{
				{Asset: "native", Balance: amount.String(a.AccountEntry.Balance)},
			}
			for _, t := range a.TrustLineEntries {
				balances = append(balances, accountResponseBalance{
					Asset:   t.Asset.StringCanonical(),
					Balance: amount.String(t.Balance),
					Limit:   amount.String(t.Limit),
				})
			}
			return balances
		}(),
	}*/

	resp := accountResponse{ ID: id }
	return c.JSON(resp)
}
