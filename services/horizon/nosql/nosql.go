package nosql

import (
	"log"
	"net/http"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

const archiveURL = "https://history.stellar.org/prd/core-live/core_live_001/"

type NoSqlServer struct {
	db *BoltStore
}

func NewNoSqlApp(db *BoltStore) *NoSqlServer {
	return &NoSqlServer{db: db}
}

func (s *NoSqlServer) Run() error {
	app := fiber.New(fiber.Config{
		ErrorHandler: func(ctx *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			message := http.StatusText(http.StatusInternalServerError)
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
				message = e.Message
			}
			return ctx.Status(code).JSON(struct {
				StatusCode int    `json:"status_code"`
				Error      string `json:"error"`
			}{
				StatusCode: code,
				Error:      message,
			})
		},
	})
	app.Use(recover.New())
	app.Get("/", (&RootHandler{
		ArchiveURL: archiveURL,
	}).Handler)
	app.Get("/accounts/:id", (&AccountHandler{
		db: s.db,
	}).Handler)
	app.Get("/ledgers/:id", (&LedgerHandler{
		db: s.db,
	}).Handler)
	app.Get("/ledgers/:id/transactions", (&TransactionHandler{
		db: s.db,
	}).ByLedgerHandler)
	app.Get("/transactions/:id", (&TransactionHandler{
		db: s.db,
	}).Handler)
	//app.Get("/operations/:id", (&OperationHandler{
	//	db: s.db,
	//}).Handler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	log.Fatal(app.Listen(":" + port))
	return nil
}
