// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package payments

import (
	"time"

	"github.com/skyrings/skyring-common/tools/uuid"

	"storj.io/storj/satellite/payments/coinpayments"
)

// Credit is an entity that holds bonus balance of user, earned by depositing with storj coins.
type Credit struct {
	UserID        uuid.UUID                  `json:"userId"`
	Amount        int64                      `json:"credit"`
	TransactionID coinpayments.TransactionID `json:"transactionId"`
	Created       time.Time                  `json:"created"`
}

// CreditsPage holds set of credits and indicates if
// there are more credits to fetch.
type CreditsPage struct {
	Credits    []Credit
	Next       bool
	NextOffset int64
}
