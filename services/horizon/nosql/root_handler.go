package nosql

import "github.com/gofiber/fiber/v2"

type RootHandler struct {
	ArchiveURL string
}

type rootResponse struct {
	CoreVersion       string `json:"core_version"`
	CoreLatestLedger  uint32 `json:"core_latest_ledger"`
	NetworkPassphrase string `json:"network_passphrase"`
}

func (h *RootHandler) Handler(c *fiber.Ctx) error {
	resp := rootResponse{
	}
	return c.JSON(resp)
}