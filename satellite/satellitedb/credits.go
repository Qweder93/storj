// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package satellitedb

import (
	"context"
	"time"

	"github.com/skyrings/skyring-common/tools/uuid"
	"github.com/zeebo/errs"

	"storj.io/storj/private/dbutil"
	"storj.io/storj/satellite/payments"
	"storj.io/storj/satellite/payments/coinpayments"
	"storj.io/storj/satellite/payments/stripecoinpayments"
	dbx "storj.io/storj/satellite/satellitedb/dbx"
)

// ensures that credit implements payments.CreditsDB.
var _ stripecoinpayments.CreditsDB = (*credit)(nil)

// credit is an implementation of payments.CreditsDB.
//
// architecture: Database
type credit struct {
	db *satelliteDB
}

// InsertCredit inserts credit into the database.
func (credits *credit) InsertCredit(ctx context.Context, credit payments.Credit) (err error) {
	defer mon.Task()(&ctx, credit)(&err)

	_, err = credits.db.Create_Credit(
		ctx,
		dbx.Credit_UserId(credit.UserID[:]),
		dbx.Credit_TransactionId(string(credit.TransactionID[:])),
		dbx.Credit_Amount(credit.Amount),
	)

	return err
}

// GetCredit returns credit by transactionID.
func (credits *credit) GetCredit(ctx context.Context, transactionID coinpayments.TransactionID) (_ payments.Credit, err error) {
	defer mon.Task()(&ctx, transactionID)(&err)

	dbxCredit, err := credits.db.Get_Credit_By_TransactionId(ctx, dbx.Credit_TransactionId(string(transactionID)))
	if err != nil {
		return payments.Credit{}, err
	}

	return fromDBXCredit(dbxCredit)
}

// ListCredits returns all credits of specified user.
func (credits *credit) ListCredits(ctx context.Context, userID uuid.UUID) (_ []payments.Credit, err error) {
	defer mon.Task()(&ctx, userID)(&err)

	dbxCredits, err := credits.db.All_Credit_By_UserId_OrderBy_Desc_CreatedAt(
		ctx,
		dbx.Credit_UserId(userID[:]),
	)
	if err != nil {
		return nil, err
	}

	return creditsFromDbxSlice(dbxCredits)
}

// ListCreditsPaged returns paginated list of user's credits.
func (credits *credit) ListCreditsPaged(ctx context.Context, offset int64, limit int, before time.Time, userID uuid.UUID) (_ payments.CreditsPage, err error) {
	defer mon.Task()(&ctx)(&err)

	var page payments.CreditsPage

	dbxCredits, err := credits.db.Limited_Credit_By_UserId_And_CreatedAt_LessOrEqual_OrderBy_Desc_CreatedAt(
		ctx,
		dbx.Credit_UserId(userID[:]),
		dbx.Credit_CreatedAt(before.UTC()),
		limit+1,
		offset,
	)
	if err != nil {
		return payments.CreditsPage{}, err
	}

	if len(dbxCredits) == limit+1 {
		page.Next = true
		page.NextOffset = offset + int64(limit) + 1

		dbxCredits = dbxCredits[:len(dbxCredits)-1]
	}

	page.Credits, err = creditsFromDbxSlice(dbxCredits)
	if err != nil {
		return payments.CreditsPage{}, nil
	}

	return page, nil
}

// Insert inserts spending into the database.
func (credits *credit) InsertSpending(ctx context.Context, spending stripecoinpayments.Spending) (err error) {
	defer mon.Task()(&ctx, spending)(&err)

	_, err = credits.db.Create_Spending(
		ctx,
		dbx.Spending_Id(spending.ID[:]),
		dbx.Spending_UserId(spending.UserID[:]),
		dbx.Spending_ProjectId(spending.ProjectID[:]),
		dbx.Spending_Amount(spending.Amount),
		dbx.Spending_Status(spending.Status),
	)

	return err
}

// Get returns spending by ID.
func (credits *credit) GetSpending(ctx context.Context, id uuid.UUID) (_ stripecoinpayments.Spending, err error) {
	defer mon.Task()(&ctx, id)(&err)

	dbxSpending, err := credits.db.Get_Spending_By_Id(ctx, dbx.Spending_Id(id[:]))
	if err != nil {
		return stripecoinpayments.Spending{}, err
	}

	return fromDBXSpending(dbxSpending)
}

// List returns all spendings of specified user.
func (credits *credit) ListSpendings(ctx context.Context, userID uuid.UUID) (_ []stripecoinpayments.Spending, err error) {
	defer mon.Task()(&ctx, userID)(&err)

	dbxSpendings, err := credits.db.All_Spending_By_UserId_OrderBy_Desc_CreatedAt(
		ctx,
		dbx.Spending_UserId(userID[:]),
	)
	if err != nil {
		return nil, err
	}

	return spendingsFromDbxSlice(dbxSpendings)
}

// ApplyUsage applies spending and updates its status.
func (credits *credit) ApplySpending(ctx context.Context, spendingID uuid.UUID, status int) (err error) {
	defer mon.Task()(&ctx)(&err)

	_, err = credits.db.Update_Spending_By_Id(
		ctx,
		dbx.Spending_Id(spendingID[:]),
		dbx.Spending_Update_Fields{Status: dbx.Spending_Status(status)},
	)

	return err
}

// ListPending returns paginated list of user's spendings.
func (credits *credit) ListSpendingsPaged(ctx context.Context, status int, offset int64, limit int, before time.Time) (_ stripecoinpayments.SpendingsPage, err error) {
	defer mon.Task()(&ctx)(&err)

	var page stripecoinpayments.SpendingsPage

	dbxSpendings, err := credits.db.Limited_Spending_By_CreatedAt_LessOrEqual_And_Status_OrderBy_Desc_CreatedAt(
		ctx,
		dbx.Spending_CreatedAt(before.UTC()),
		dbx.Spending_Status(status),
		limit+1,
		offset,
	)
	if err != nil {
		return stripecoinpayments.SpendingsPage{}, err
	}

	if len(dbxSpendings) == limit+1 {
		page.Next = true
		page.NextOffset = offset + int64(limit) + 1

		dbxSpendings = dbxSpendings[:len(dbxSpendings)-1]
	}

	page.Spendings, err = spendingsFromDbxSlice(dbxSpendings)
	if err != nil {
		return stripecoinpayments.SpendingsPage{}, nil
	}

	return page, nil
}

// Balance returns difference between earned for deposit and spent on invoices credits.
func (credits *credit) Balance(ctx context.Context, userID uuid.UUID) (balance int64, err error) {
	defer mon.Task()(&ctx)(&err)
	var creditsAmount, spendingsAmount int64

	allCredits, err := credits.ListCredits(ctx, userID)
	if err != nil {
		return 0, err
	}

	allSpendings, err := credits.ListSpendings(ctx, userID)
	if err != nil {
		return 0, err
	}

	for i := range allCredits {
		creditsAmount += allCredits[i].Amount
	}

	for j := range allSpendings {
		spendingsAmount += allSpendings[j].Amount
	}

	balance = creditsAmount - spendingsAmount
	if balance < 0 {
		return 0, Error.New("credit balance can't be < 0")
	}

	return balance, nil
}

// fromDBXCredit converts *dbx.Credit to *payments.Credit.
func fromDBXCredit(dbxCredit *dbx.Credit) (credit payments.Credit, err error) {
	credit.TransactionID = coinpayments.TransactionID(dbxCredit.TransactionId)
	credit.UserID, err = dbutil.BytesToUUID(dbxCredit.UserId)
	if err != nil {
		return payments.Credit{}, err
	}

	credit.Created = dbxCredit.CreatedAt
	credit.Amount = dbxCredit.Amount

	return credit, nil
}

// creditsFromDbxSlice is used for creating []payments.Credits entities from autogenerated []dbx.Credits struct.
func creditsFromDbxSlice(creditsDbx []*dbx.Credit) (_ []payments.Credit, err error) {
	var credits = make([]payments.Credit, 0)
	var errors []error

	// Generating []dbo from []dbx and collecting all errors
	for _, creditDbx := range creditsDbx {
		credit, err := fromDBXCredit(creditDbx)
		if err != nil {
			errors = append(errors, err)
			continue
		}

		credits = append(credits, credit)
	}

	return credits, errs.Combine(errors...)
}

// fromDBXSpending converts *dbx.Spending to *payments.Spending.
func fromDBXSpending(dbxSpending *dbx.Spending) (spending stripecoinpayments.Spending, err error) {
	spending.UserID, err = dbutil.BytesToUUID(dbxSpending.UserId)
	if err != nil {
		return stripecoinpayments.Spending{}, err
	}

	spending.ProjectID, err = dbutil.BytesToUUID(dbxSpending.ProjectId)
	if err != nil {
		return stripecoinpayments.Spending{}, err
	}

	spending.Status = dbxSpending.Status
	spending.Created = dbxSpending.CreatedAt
	spending.Amount = dbxSpending.Amount

	return spending, nil
}

// spendingsFromDbxSlice is used for creating []payments.Spendings entities from autogenerated []dbx.Spending struct.
func spendingsFromDbxSlice(spendingsDbx []*dbx.Spending) (_ []stripecoinpayments.Spending, err error) {
	var spendings = make([]stripecoinpayments.Spending, 0)
	var errors []error

	// Generating []dbo from []dbx and collecting all errors
	for _, spendingDbx := range spendingsDbx {
		spending, err := fromDBXSpending(spendingDbx)
		if err != nil {
			errors = append(errors, err)
			continue
		}

		spendings = append(spendings, spending)
	}

	return spendings, errs.Combine(errors...)
}
