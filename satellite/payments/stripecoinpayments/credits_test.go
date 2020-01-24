// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package stripecoinpayments_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/payments"
	"storj.io/storj/satellite/payments/stripecoinpayments"
	"storj.io/storj/satellite/satellitedb/satellitedbtest"
)

func TestCreditsRepository(t *testing.T) {
	satellitedbtest.Run(t, func(ctx *testcontext.Context, t *testing.T, db satellite.DB) {
		creditsRepo := db.StripeCoinPayments().Credits()
		userID := testrand.UUID()
		credit := payments.Credit{
			UserID:        userID,
			Amount:        10,
			TransactionID: "transactionID",
		}

		spending := stripecoinpayments.Spending{
			ProjectID: testrand.UUID(),
			UserID:    userID,
			Amount:    5,
			Status:    0,
		}

		t.Run("insert credit", func(t *testing.T) {
			err := creditsRepo.InsertCredit(ctx, credit)
			assert.NoError(t, err)

			credits, err := creditsRepo.ListCredits(ctx, userID)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(credits))
			credit = credits[0]
		})

		t.Run("get credit by transactionID", func(t *testing.T) {
			crdt, err := creditsRepo.GetCredit(ctx, credit.TransactionID)
			assert.NoError(t, err)
			assert.Equal(t, int64(10), crdt.Amount)
		})

		t.Run("insert spending", func(t *testing.T) {
			err := creditsRepo.InsertSpending(ctx, spending)
			assert.NoError(t, err)

			spendings, err := creditsRepo.ListSpendings(ctx, userID)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(spendings))
		})

		t.Run("update spending", func(t *testing.T) {
			err := creditsRepo.ApplySpending(ctx, spending.ID, 1)
			assert.NoError(t, err)
		})

		t.Run("balance", func(t *testing.T) {
			balance, err := creditsRepo.Balance(ctx, userID)
			assert.NoError(t, err)
			assert.Equal(t, 5, int(balance))
		})
	})
}
