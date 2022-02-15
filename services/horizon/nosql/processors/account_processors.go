package processors

import (
	"github.com/guregu/null"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type AccountsProcessor struct {
	Pre *xdr.LedgerEntry
	Post *xdr.LedgerEntry
}

func NewAccountProcessor(
	chg *ingest.Change,
) (*AccountsProcessor, error) {
	if chg.Type != xdr.LedgerEntryTypeAccount {
	return nil, errors.Errorf("not of type account")
}

	return &AccountsProcessor{
		Pre: chg.Pre,
		Post: chg.Post,
	}, nil
}

func (p *AccountsProcessor) GetAccount() (map[string]interface{}, error) {
	if p.Post != nil {
		return accountMap(*p.Post), nil
	}
	return map[string]interface{}{}, nil
}

func accountMap(entry xdr.LedgerEntry) map[string]interface{} {
	account := entry.Data.MustAccount()
	liabilities := account.Liabilities()

	var inflationDestination = ""
	if account.InflationDest != nil {
		inflationDestination = account.InflationDest.Address()
	}

	return map[string]interface{}{
		"account_id":            account.AccountId.Address(),
		"balance":               account.Balance,
		"buying_liabilities":    liabilities.Buying,
		"selling_liabilities":   liabilities.Selling,
		"sequence_number":       account.SeqNum,
		"num_subentries":        account.NumSubEntries,
		"inflation_destination": inflationDestination,
		"flags":                 account.Flags,
		"home_domain":           account.HomeDomain,
		"master_weight":         account.MasterKeyWeight(),
		"threshold_low":         account.ThresholdLow(),
		"threshold_medium":      account.ThresholdMedium(),
		"threshold_high":        account.ThresholdHigh(),
		"last_modified_ledger":  entry.LastModifiedLedgerSeq,
		"sponsor":               ledgerEntrySponsorToNullString(entry),
		"num_sponsored":         account.NumSponsored(),
		"num_sponsoring":        account.NumSponsoring(),
	}
}

func ledgerEntrySponsorToNullString(entry xdr.LedgerEntry) null.String {
	sponsoringID := entry.SponsoringID()

	var sponsor null.String
	if sponsoringID != nil {
		sponsor.SetValid((*sponsoringID).Address())
	}

	return sponsor
}