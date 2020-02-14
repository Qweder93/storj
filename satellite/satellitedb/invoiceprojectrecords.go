// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package satellitedb

import (
	"context"
	"database/sql"
	"time"

	"github.com/skyrings/skyring-common/tools/uuid"
	"github.com/zeebo/errs"

	"storj.io/storj/private/dbutil"
	"storj.io/storj/satellite/payments/stripecoinpayments"
	"storj.io/storj/satellite/satellitedb/dbx"
)

// ensure that invoiceProjectRecords implements stripecoinpayments.ProjectRecordsDB.
var _ stripecoinpayments.ProjectRecordsDB = (*invoiceProjectRecords)(nil)

// invoiceProjectRecordState defines states of the invoice project record.
type invoiceProjectRecordState int

const (
	// invoice project record is not yet applied to customer invoice.
	invoiceProjectRecordStateUnapplied invoiceProjectRecordState = 0
	// invoice project record has been used during creating customer invoice.
	invoiceProjectRecordStateConsumed invoiceProjectRecordState = 1
)

// Int returns intent state as int.
func (intent invoiceProjectRecordState) Int() int {
	return int(intent)
}

// invoiceProjectRecords is stripecoinpayments project records DB.
//
// architecture: Database
type invoiceProjectRecords struct {
	db *satelliteDB
}

// Create creates new invoice project record in the DB.
func (db *invoiceProjectRecords) Create(ctx context.Context, records []stripecoinpayments.CreateProjectRecord, couponUsages []stripecoinpayments.CouponUsage, creditsSpendings []stripecoinpayments.CreditsSpending, start, end time.Time) (err error) {
	defer mon.Task()(&ctx)(&err)

	return db.db.WithTx(ctx, func(ctx context.Context, tx *dbx.Tx) error {
		for _, record := range records {
			id, err := uuid.New()
			if err != nil {
				return Error.Wrap(err)
			}

			_, err = db.db.Create_StripecoinpaymentsInvoiceProjectRecord(ctx,
				dbx.StripecoinpaymentsInvoiceProjectRecord_Id(id[:]),
				dbx.StripecoinpaymentsInvoiceProjectRecord_ProjectId(record.ProjectID[:]),
				dbx.StripecoinpaymentsInvoiceProjectRecord_Storage(record.Storage),
				dbx.StripecoinpaymentsInvoiceProjectRecord_Egress(record.Egress),
				dbx.StripecoinpaymentsInvoiceProjectRecord_Objects(int64(record.Objects)),
				dbx.StripecoinpaymentsInvoiceProjectRecord_PeriodStart(start),
				dbx.StripecoinpaymentsInvoiceProjectRecord_PeriodEnd(end),
				dbx.StripecoinpaymentsInvoiceProjectRecord_State(invoiceProjectRecordStateUnapplied.Int()),
			)
			if err != nil {
				return err
			}
		}

		for _, couponUsage := range couponUsages {
			_, err = db.db.Create_CouponUsage(
				ctx,
				dbx.CouponUsage_CouponId(couponUsage.CouponID[:]),
				dbx.CouponUsage_Amount(couponUsage.Amount),
				dbx.CouponUsage_Status(int(couponUsage.Status)),
				dbx.CouponUsage_Period(couponUsage.Period),
			)
			if err != nil {
				return err
			}
		}

		for _, creditsSpending := range creditsSpendings {
			_, err = db.db.Create_CreditsSpending(
				ctx,
				dbx.CreditsSpending_Id(creditsSpending.ID[:]),
				dbx.CreditsSpending_UserId(creditsSpending.UserID[:]),
				dbx.CreditsSpending_ProjectId(creditsSpending.ProjectID[:]),
				dbx.CreditsSpending_Amount(creditsSpending.Amount),
				dbx.CreditsSpending_Status(int(creditsSpending.Status)),
			)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// Check checks if invoice project record for specified project and billing period exists.
func (db *invoiceProjectRecords) Check(ctx context.Context, projectID uuid.UUID, start, end time.Time) (err error) {
	defer mon.Task()(&ctx)(&err)

	_, err = db.db.Get_StripecoinpaymentsInvoiceProjectRecord_By_ProjectId_And_PeriodStart_And_PeriodEnd(ctx,
		dbx.StripecoinpaymentsInvoiceProjectRecord_ProjectId(projectID[:]),
		dbx.StripecoinpaymentsInvoiceProjectRecord_PeriodStart(start),
		dbx.StripecoinpaymentsInvoiceProjectRecord_PeriodEnd(end),
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}

		return err
	}

	return stripecoinpayments.ErrProjectRecordExists
}

// Get returns record for specified project and billing period.
func (db *invoiceProjectRecords) Get(ctx context.Context, projectID uuid.UUID, start, end time.Time) (record *stripecoinpayments.ProjectRecord, err error) {
	defer mon.Task()(&ctx)(&err)

	dbxRecord, err := db.db.Get_StripecoinpaymentsInvoiceProjectRecord_By_ProjectId_And_PeriodStart_And_PeriodEnd(ctx,
		dbx.StripecoinpaymentsInvoiceProjectRecord_ProjectId(projectID[:]),
		dbx.StripecoinpaymentsInvoiceProjectRecord_PeriodStart(start),
		dbx.StripecoinpaymentsInvoiceProjectRecord_PeriodEnd(end),
	)

	if err != nil {
		return nil, err
	}

	return fromDBXInvoiceProjectRecord(dbxRecord)
}

// Consume consumes invoice project record.
func (db *invoiceProjectRecords) Consume(ctx context.Context, id uuid.UUID) (err error) {
	defer mon.Task()(&ctx)(&err)

	_, err = db.db.Update_StripecoinpaymentsInvoiceProjectRecord_By_Id(ctx,
		dbx.StripecoinpaymentsInvoiceProjectRecord_Id(id[:]),
		dbx.StripecoinpaymentsInvoiceProjectRecord_Update_Fields{
			State: dbx.StripecoinpaymentsInvoiceProjectRecord_State(invoiceProjectRecordStateConsumed.Int()),
		},
	)

	return err
}

// ListUnapplied returns project records page with unapplied project records.
func (db *invoiceProjectRecords) ListUnapplied(ctx context.Context, offset int64, limit int, before time.Time) (_ stripecoinpayments.ProjectRecordsPage, err error) {
	defer mon.Task()(&ctx)(&err)

	var page stripecoinpayments.ProjectRecordsPage

	dbxRecords, err := db.db.Limited_StripecoinpaymentsInvoiceProjectRecord_By_CreatedAt_LessOrEqual_And_State_OrderBy_Desc_CreatedAt(ctx,
		dbx.StripecoinpaymentsInvoiceProjectRecord_CreatedAt(before),
		dbx.StripecoinpaymentsInvoiceProjectRecord_State(invoiceProjectRecordStateUnapplied.Int()),
		limit+1,
		offset,
	)
	if err != nil {
		return stripecoinpayments.ProjectRecordsPage{}, err
	}

	if len(dbxRecords) == limit+1 {
		page.Next = true
		page.NextOffset = offset + int64(limit) + 1

		dbxRecords = dbxRecords[:len(dbxRecords)-1]
	}

	for _, dbxRecord := range dbxRecords {
		record, err := fromDBXInvoiceProjectRecord(dbxRecord)
		if err != nil {
			return stripecoinpayments.ProjectRecordsPage{}, err
		}

		page.Records = append(page.Records, *record)
	}

	return page, nil
}

// fromDBXInvoiceProjectRecord converts *dbx.StripecoinpaymentsInvoiceProjectRecord to *stripecoinpayments.ProjectRecord
func fromDBXInvoiceProjectRecord(dbxRecord *dbx.StripecoinpaymentsInvoiceProjectRecord) (*stripecoinpayments.ProjectRecord, error) {
	id, err := dbutil.BytesToUUID(dbxRecord.Id)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	projectID, err := dbutil.BytesToUUID(dbxRecord.ProjectId)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return &stripecoinpayments.ProjectRecord{
		ID:          id,
		ProjectID:   projectID,
		Storage:     dbxRecord.Storage,
		Egress:      dbxRecord.Egress,
		Objects:     float64(dbxRecord.Objects),
		PeriodStart: dbxRecord.PeriodStart,
		PeriodEnd:   dbxRecord.PeriodEnd,
	}, nil
}
