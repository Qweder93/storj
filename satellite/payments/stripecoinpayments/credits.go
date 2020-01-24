// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package stripecoinpayments

import (
	"context"
	"time"

	"github.com/skyrings/skyring-common/tools/uuid"

	"storj.io/storj/satellite/payments"
	"storj.io/storj/satellite/payments/coinpayments"
)

// CreditsDB is an interface for managing credits table.
//
// architecture: Database
type CreditsDB interface {
	// InsertCredit inserts credit to user's credit balance into the database.
	InsertCredit(ctx context.Context, credit payments.Credit) error
	// GetCredit returns credit by transactionID.
	GetCredit(ctx context.Context, transactionID coinpayments.TransactionID) (_ payments.Credit, err error)
	// ListCredits returns all credits of specific user.
	ListCredits(ctx context.Context, userID uuid.UUID) ([]payments.Credit, error)
	// ListCreditsPaged returns all credits of specific user.
	ListCreditsPaged(ctx context.Context, offset int64, limit int, before time.Time, userID uuid.UUID) (payments.CreditsPage, error)

	// InsertSpending inserts spending to user's spending list into the database.
	InsertSpending(ctx context.Context, spending Spending) error
	// GetSpending returns spending by ID.
	GetSpending(ctx context.Context, spendingID uuid.UUID) (Spending, error)
	// ListSpendings returns spending received for concrete deposit.
	ListSpendings(ctx context.Context, userID uuid.UUID) ([]Spending, error)
	// ListSpendingsPaged returns all spending of specific user.
	ListSpendingsPaged(ctx context.Context, status int, offset int64, limit int, before time.Time) (SpendingsPage, error)
	// ApplySpending updated spending's status.
	ApplySpending(ctx context.Context, spendingID uuid.UUID, status int) (err error)

	// Balance returns difference between all credits and spendings of specific user.
	Balance(ctx context.Context, userID uuid.UUID) (int64, error)
}

// Spending is an entity that holds funds been used from Accounts bonus credit balance.
// Status shows if spending have been used to pay for invoice already or not.
type Spending struct {
	ID        uuid.UUID `json:"id"`
	ProjectID uuid.UUID `json:"projectId"`
	UserID    uuid.UUID `json:"userId"`
	Amount    int64     `json:"amount"`
	Status    int       `json:"status"`
	Created   time.Time `json:"created"`
}

// SpendingsPage holds set of spendings and indicates if
// there are more spendings to fetch.
type SpendingsPage struct {
	Spendings  []Spending
	Next       bool
	NextOffset int64
}

// SpendingStatus indicates the state of the spending.
type SpendingStatus int

const (
	// SpendingPrepared is a default spending state.
	SpendingPrepared SpendingStatus = 0
	// SpendingApplied status indicates that spending was applied.
	SpendingApplied SpendingStatus = 1
)
