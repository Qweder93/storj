// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package satellitedb

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/zeebo/errs"

	"storj.io/storj/private/dbutil"
	"storj.io/storj/private/dbutil/pgutil"
	"storj.io/storj/private/migrate"
)

var (
	// ErrMigrate is for tracking migration errors
	ErrMigrate = errs.Class("migrate")
	// ErrMigrateMinVersion is for migration min version errors
	ErrMigrateMinVersion = errs.Class("migrate min version")
)

// CreateTables is a method for creating all tables for database
func (db *satelliteDB) CreateTables(ctx context.Context) error {
	// First handle the idiosyncrasies of postgres and cockroach migrations. Postgres
	// will need to create any schemas specified in the search path, and cockroach
	// will need to create the database it was told to connect to. These things should
	// not really be here, and instead should be assumed to exist.
	// This is tracked in jira ticket #3338.
	switch db.implementation {
	case dbutil.Postgres:
		schema, err := pgutil.ParseSchemaFromConnstr(db.source)
		if err != nil {
			return errs.New("error parsing schema: %+v", err)
		}

		if schema != "" {
			err = pgutil.CreateSchema(ctx, db, schema)
			if err != nil {
				return errs.New("error creating schema: %+v", err)
			}
		}

	case dbutil.Cockroach:
		var dbName string
		if err := db.QueryRow(ctx, `SELECT current_database();`).Scan(&dbName); err != nil {
			return errs.New("error querying current database: %+v", err)
		}

		_, err := db.Exec(ctx, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s;`,
			pq.QuoteIdentifier(dbName)))
		if err != nil {
			return errs.Wrap(err)
		}
	}

	switch db.implementation {
	case dbutil.Postgres, dbutil.Cockroach:
		migration := db.PostgresMigration()
		// since we merged migration steps 0-69, the current db version should never be
		// less than 69 unless the migration hasn't run yet
		const minDBVersion = 69
		dbVersion, err := migration.CurrentVersion(ctx, db.log, db.DB)
		if err != nil {
			return errs.New("error current version: %+v", err)
		}
		if dbVersion > -1 && dbVersion < minDBVersion {
			return ErrMigrateMinVersion.New("current database version is %d, it shouldn't be less than the min version %d",
				dbVersion, minDBVersion,
			)
		}

		return migration.Run(ctx, db.log.Named("migrate"))
	default:
		return migrate.Create(ctx, "database", db.DB)
	}
}

// TestingCreateTables is a method for creating all tables for database for testing.
func (db *satelliteDB) TestingCreateTables(ctx context.Context) error {
	switch db.implementation {
	case dbutil.Postgres:
		schema, err := pgutil.ParseSchemaFromConnstr(db.source)
		if err != nil {
			return ErrMigrateMinVersion.New("error parsing schema: %+v", err)
		}

		if schema != "" {
			err = pgutil.CreateSchema(ctx, db, schema)
			if err != nil {
				return ErrMigrateMinVersion.New("error creating schema: %+v", err)
			}
		}

	case dbutil.Cockroach:
		var dbName string
		if err := db.QueryRow(ctx, `SELECT current_database();`).Scan(&dbName); err != nil {
			return ErrMigrateMinVersion.New("error querying current database: %+v", err)
		}

		_, err := db.Exec(ctx, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s;`, pq.QuoteIdentifier(dbName)))
		if err != nil {
			return ErrMigrateMinVersion.Wrap(err)
		}
	}

	switch db.implementation {
	case dbutil.Postgres, dbutil.Cockroach:
		migration := db.PostgresMigration()

		dbVersion, err := migration.CurrentVersion(ctx, db.log, db.DB)
		if err != nil {
			return ErrMigrateMinVersion.Wrap(err)
		}
		if dbVersion > -1 {
			return ErrMigrateMinVersion.New("the database must be empty, got version %d", dbVersion)
		}

		flattened, err := flattenMigration(migration)
		if err != nil {
			return ErrMigrateMinVersion.Wrap(err)
		}

		return flattened.Run(ctx, db.log.Named("migrate"))
	default:
		return migrate.Create(ctx, "database", db.DB)
	}
}

// CheckVersion confirms the database is at the desired version
func (db *satelliteDB) CheckVersion(ctx context.Context) error {
	switch db.implementation {
	case dbutil.Postgres, dbutil.Cockroach:
		migration := db.PostgresMigration()
		return migration.ValidateVersions(ctx, db.log)

	default:
		return nil
	}
}

func flattenMigration(m *migrate.Migration) (*migrate.Migration, error) {
	var db migrate.DB
	var version int
	var statements migrate.SQL

	for _, step := range m.Steps {
		if db == nil {
			db = step.DB
		} else if db != step.DB {
			return nil, errs.New("multiple databases not supported")
		}

		version = step.Version
		sql, ok := step.Action.(migrate.SQL)
		if !ok {
			return nil, errs.New("unexpected action type %T", step.Action)
		}

		statements = append(statements, sql...)
	}

	return &migrate.Migration{
		Table: "versions",
		Steps: []*migrate.Step{
			{
				DB:          db,
				Description: "Setup",
				Version:     version,
				Action:      migrate.SQL{strings.Join(statements, ";\n")},
			},
		},
	}, nil
}

// PostgresMigration returns steps needed for migrating postgres database.
func (db *satelliteDB) PostgresMigration() *migrate.Migration {
	return &migrate.Migration{
		Table: "versions",
		Steps: []*migrate.Step{
			{
				DB:          db.DB,
				Description: "Initial setup",
				Version:     69,
				Action: migrate.SQL{
					`CREATE TABLE accounting_rollups (
						id bigserial NOT NULL,
						node_id bytea NOT NULL,
						start_time timestamp with time zone NOT NULL,
						put_total bigint NOT NULL,
						get_total bigint NOT NULL,
						get_audit_total bigint NOT NULL,
						get_repair_total bigint NOT NULL,
						put_repair_total bigint NOT NULL,
						at_rest_total double precision NOT NULL,
						PRIMARY KEY ( id )
					);`,

					`CREATE TABLE accounting_timestamps (
						name text NOT NULL,
						value timestamp with time zone NOT NULL,
						PRIMARY KEY ( name )
					);`,

					`CREATE TABLE bucket_bandwidth_rollups (
						bucket_name bytea NOT NULL,
						interval_start timestamp NOT NULL,
						interval_seconds integer NOT NULL,
						action integer NOT NULL,
						inline bigint NOT NULL,
						allocated bigint NOT NULL,
						settled bigint NOT NULL,
						project_id bytea NOT NULL ,
						CONSTRAINT bucket_bandwidth_rollups_pk PRIMARY KEY (bucket_name, project_id, interval_start, action)
					);`,
					`CREATE INDEX bucket_name_project_id_interval_start_interval_seconds ON bucket_bandwidth_rollups ( bucket_name, project_id, interval_start, interval_seconds );`,

					`CREATE TABLE bucket_storage_tallies (
						bucket_name bytea NOT NULL,
						interval_start timestamp NOT NULL,
						inline bigint NOT NULL,
						remote bigint NOT NULL,
						remote_segments_count integer NOT NULL,
						inline_segments_count integer NOT NULL,
						object_count integer NOT NULL,
						metadata_size bigint NOT NULL,
						project_id bytea NOT NULL,
						CONSTRAINT bucket_storage_tallies_pk PRIMARY KEY (bucket_name, project_id, interval_start)
					);`,

					`CREATE TABLE injuredsegments (
						data bytea NOT NULL,
						attempted timestamp,
						path bytea NOT NULL,
						CONSTRAINT injuredsegments_pk PRIMARY KEY (path)
					);`,
					`CREATE INDEX injuredsegments_attempted_index ON injuredsegments ( attempted );`,

					`CREATE TABLE irreparabledbs (
						segmentpath bytea NOT NULL,
						segmentdetail bytea NOT NULL,
						pieces_lost_count bigint NOT NULL,
						seg_damaged_unix_sec bigint NOT NULL,
						repair_attempt_count bigint NOT NULL,
						PRIMARY KEY ( segmentpath )
					);`,

					`CREATE TABLE nodes (
						id bytea NOT NULL,
						audit_success_count bigint NOT NULL DEFAULT 0,
						total_audit_count bigint NOT NULL DEFAULT 0,
						uptime_success_count bigint NOT NULL,
						total_uptime_count bigint NOT NULL,
						created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
						updated_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
						wallet text NOT NULL,
						email text NOT NULL,
						address text NOT NULL DEFAULT '',
						protocol INTEGER NOT NULL DEFAULT 0,
						type INTEGER NOT NULL DEFAULT 0,
						free_bandwidth BIGINT NOT NULL DEFAULT -1,
						free_disk BIGINT NOT NULL DEFAULT -1,
						latency_90 BIGINT NOT NULL DEFAULT 0,
						last_contact_success TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT 'epoch',
						last_contact_failure TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT 'epoch',
						major bigint NOT NULL DEFAULT 0,
						minor bigint NOT NULL DEFAULT 0,
						patch bigint NOT NULL DEFAULT 0,
						hash TEXT NOT NULL DEFAULT '',
						timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT '0001-01-01 00:00:00+00',
						release bool NOT NULL DEFAULT FALSE,
						contained bool NOT NULL DEFAULT FALSE,
						last_net text NOT NULL,
						disqualified timestamp with time zone,
						audit_reputation_alpha double precision NOT NULL DEFAULT 1,
						audit_reputation_beta double precision NOT NULL DEFAULT 0,
						uptime_reputation_alpha double precision NOT NULL DEFAULT 1,
						uptime_reputation_beta double precision NOT NULL DEFAULT 0,
						piece_count bigint NOT NULL DEFAULT 0,
						exit_loop_completed_at TIMESTAMP,
						exit_initiated_at TIMESTAMP,
						exit_finished_at TIMESTAMP,
						exit_success boolean NOT NULL DEFAULT FALSE,
						PRIMARY KEY ( id )
					);`,
					`CREATE INDEX node_last_ip ON nodes ( last_net );`,

					`CREATE TABLE offers (
						id serial NOT NULL,
						name text NOT NULL,
						description text NOT NULL,
						type integer NOT NULL,
						award_credit_duration_days integer,
						invitee_credit_duration_days integer,
						redeemable_cap integer,
						expires_at timestamp with time zone NOT NULL,
						created_at timestamp with time zone NOT NULL,
						status integer NOT NULL,
						award_credit_in_cents integer NOT NULL DEFAULT 0,
						invitee_credit_in_cents integer NOT NULL DEFAULT 0,
						PRIMARY KEY ( id )
					);`,

					`CREATE TABLE peer_identities (
						node_id bytea NOT NULL,
						leaf_serial_number bytea NOT NULL,
						chain bytea NOT NULL,
						updated_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( node_id )
					);`,

					`CREATE TABLE pending_audits (
						node_id bytea NOT NULL,
						piece_id bytea NOT NULL,
						stripe_index bigint NOT NULL,
						share_size bigint NOT NULL,
						expected_share_hash bytea NOT NULL,
						reverify_count bigint NOT NULL,
						path bytea NOT NULL,
						PRIMARY KEY ( node_id )
					);`,

					`CREATE TABLE projects (
						id bytea NOT NULL,
						name text NOT NULL,
						description text NOT NULL,
						created_at timestamp with time zone NOT NULL,
						usage_limit bigint NOT NULL DEFAULT 0,
						partner_id bytea,
						owner_id bytea NOT NULL,
						PRIMARY KEY ( id )
					);`,

					`CREATE TABLE registration_tokens (
						secret bytea NOT NULL,
						owner_id bytea UNIQUE,
						project_limit integer NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( secret )
					);`,

					`CREATE TABLE reset_password_tokens (
						secret bytea NOT NULL,
						owner_id bytea NOT NULL UNIQUE,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( secret )
					);`,

					`CREATE TABLE serial_numbers (
						id serial NOT NULL,
						serial_number bytea NOT NULL,
						bucket_id bytea NOT NULL,
						expires_at timestamp NOT NULL,
						PRIMARY KEY ( id )
					);`,
					`CREATE INDEX serial_numbers_expires_at_index ON serial_numbers ( expires_at );`,
					`CREATE UNIQUE INDEX serial_number_index ON serial_numbers ( serial_number )`,

					`CREATE TABLE storagenode_bandwidth_rollups (
						storagenode_id bytea NOT NULL,
						interval_start timestamp NOT NULL,
						interval_seconds integer NOT NULL,
						action integer NOT NULL,
						allocated bigint NOT NULL,
						settled bigint NOT NULL,
						PRIMARY KEY ( storagenode_id, interval_start, action )
					);`,
					`CREATE INDEX storagenode_id_interval_start_interval_seconds_index ON storagenode_bandwidth_rollups ( storagenode_id, interval_start, interval_seconds );`,

					`CREATE TABLE accounting_raws (
						id bigserial NOT NULL,
						node_id bytea NOT NULL,
						interval_end_time timestamp with time zone NOT NULL,
						data_total double precision NOT NULL,
						PRIMARY KEY ( id )
					)`,
					`ALTER TABLE accounting_raws RENAME TO storagenode_storage_tallies`,

					`CREATE TABLE users (
						id bytea NOT NULL,
						full_name text NOT NULL,
						short_name text,
						email text NOT NULL,
						password_hash bytea NOT NULL,
						status integer NOT NULL,
						created_at timestamp with time zone NOT NULL,
						partner_id bytea,
						normalized_email text NOT NULL,
						PRIMARY KEY ( id )
					);`,

					`CREATE TABLE value_attributions (
						bucket_name bytea NOT NULL,
						partner_id bytea NOT NULL,
						last_updated timestamp NOT NULL,
						project_id bytea NOT NULL,
						PRIMARY KEY (project_id, bucket_name)
					);`,

					`CREATE TABLE api_keys (
						id bytea NOT NULL,
						project_id bytea NOT NULL REFERENCES projects( id ) ON DELETE CASCADE,
						head bytea NOT NULL UNIQUE,
						name text NOT NULL,
						secret bytea NOT NULL,
						created_at timestamp with time zone NOT NULL,
						partner_id bytea,
						PRIMARY KEY ( id ),
						UNIQUE ( name, project_id )
					);`,

					`CREATE TABLE bucket_metainfos (
						id bytea NOT NULL,
						project_id bytea NOT NULL REFERENCES projects( id ),
						name bytea NOT NULL,
						path_cipher integer NOT NULL,
						created_at timestamp with time zone NOT NULL,
						default_segment_size integer NOT NULL,
						default_encryption_cipher_suite integer NOT NULL,
						default_encryption_block_size integer NOT NULL,
						default_redundancy_algorithm integer NOT NULL,
						default_redundancy_share_size integer NOT NULL,
						default_redundancy_required_shares integer NOT NULL,
						default_redundancy_repair_shares integer NOT NULL,
						default_redundancy_optimal_shares integer NOT NULL,
						default_redundancy_total_shares integer NOT NULL,
						partner_id bytea,
						PRIMARY KEY ( id ),
						UNIQUE ( name, project_id )
					);`,

					`CREATE TABLE project_invoice_stamps (
						project_id bytea NOT NULL REFERENCES projects( id ) ON DELETE CASCADE,
						invoice_id bytea NOT NULL UNIQUE,
						start_date timestamp with time zone NOT NULL,
						end_date timestamp with time zone NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( project_id, start_date, end_date )
					);`,

					`CREATE TABLE project_members (
						member_id bytea NOT NULL REFERENCES users( id ) ON DELETE CASCADE,
						project_id bytea NOT NULL REFERENCES projects( id ) ON DELETE CASCADE,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( member_id, project_id )
					);`,

					`CREATE TABLE used_serials (
						serial_number_id integer NOT NULL REFERENCES serial_numbers( id ) ON DELETE CASCADE,
						storage_node_id bytea NOT NULL,
						PRIMARY KEY ( serial_number_id, storage_node_id )
					);`,

					`CREATE TABLE user_credits (
						id serial NOT NULL,
						user_id bytea NOT NULL REFERENCES users( id ) ON DELETE CASCADE,
						offer_id integer NOT NULL REFERENCES offers( id ),
						referred_by bytea REFERENCES users( id ) ON DELETE SET NULL,
						credits_earned_in_cents integer NOT NULL,
						credits_used_in_cents integer NOT NULL,
						expires_at timestamp with time zone NOT NULL,
						created_at timestamp with time zone NOT NULL,
						type text NOT NULL,
						PRIMARY KEY ( id )
					);`,
					`CREATE UNIQUE INDEX credits_earned_user_id_offer_id ON user_credits (id, offer_id);`,

					`INSERT INTO offers (
						id,
						name,
						description,
						award_credit_in_cents,
						invitee_credit_in_cents,
						expires_at,
						created_at,
						status,
						type,
						award_credit_duration_days,
						invitee_credit_duration_days
					)
					VALUES (
						1,
						'Default referral offer',
						'Is active when no other active referral offer',
						300,
						600,
						'2119-03-14 08:28:24.636949+00',
						'2019-07-14 08:28:24.636949+00',
						1,
						2,
						365,
						14
					),
					(
						2,
						'Default free credit offer',
						'Is active when no active free credit offer',
						0,
						300,
						'2119-03-14 08:28:24.636949+00',
						'2019-07-14 08:28:24.636949+00',
						1,
						1,
						NULL,
						14
					) ON CONFLICT DO NOTHING;`,

					`CREATE TABLE graceful_exit_progress (
						node_id bytea NOT NULL,
						bytes_transferred bigint NOT NULL,
						updated_at timestamp NOT NULL,
						pieces_transferred bigint NOT NULL DEFAULT 0,
						pieces_failed bigint NOT NULL DEFAULT 0,
						PRIMARY KEY ( node_id )
					);`,

					`CREATE TABLE graceful_exit_transfer_queue (
						node_id bytea NOT NULL,
						path bytea NOT NULL,
						piece_num integer NOT NULL,
						durability_ratio double precision NOT NULL,
						queued_at timestamp NOT NULL,
						requested_at timestamp,
						last_failed_at timestamp,
						last_failed_code integer,
						failed_count integer,
						finished_at timestamp,
						root_piece_id bytea,
						order_limit_send_count integer NOT NULL DEFAULT 0,
						PRIMARY KEY ( node_id, path, piece_num )
					);`,

					`CREATE TABLE stripe_customers (
						user_id bytea NOT NULL,
						customer_id text NOT NULL UNIQUE,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( user_id )
					);`,

					`CREATE TABLE stripecoinpayments_invoice_project_records (
						id bytea NOT NULL,
						project_id bytea NOT NULL,
						storage double precision NOT NULL,
						egress bigint NOT NULL,
						objects bigint NOT NULL,
						period_start timestamp with time zone NOT NULL,
						period_end timestamp with time zone NOT NULL,
						state integer NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( id ),
						UNIQUE ( project_id, period_start, period_end )
					);`,
					`CREATE TABLE stripecoinpayments_tx_conversion_rates (
						tx_id text NOT NULL,
						rate bytea NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( tx_id )
					);`,

					`CREATE TABLE coinpayments_transactions (
						id text NOT NULL,
						user_id bytea NOT NULL,
						address text NOT NULL,
						amount bytea NOT NULL,
						received bytea NOT NULL,
						status integer NOT NULL,
						key text NOT NULL,
						timeout integer NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( id )
					);`,

					`CREATE TABLE stripecoinpayments_apply_balance_intents (
						tx_id text NOT NULL REFERENCES coinpayments_transactions( id ) ON DELETE CASCADE,
						state integer NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( tx_id )
					);`,
				},
			},
			{
				DB:          db.DB,
				Description: "Add coupons and coupon_usage tables",
				Version:     70,
				Action: migrate.SQL{
					`CREATE TABLE coupons (
						id bytea NOT NULL,
						project_id bytea NOT NULL,
						user_id bytea NOT NULL,
						amount bigint NOT NULL,
						description text NOT NULL,
						status integer NOT NULL,
						duration bigint NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( id ),
						UNIQUE ( project_id )
					);`,
					`CREATE TABLE coupon_usages (
						id bytea NOT NULL,
						coupon_id bytea NOT NULL,
						amount bigint NOT NULL,
						interval_end timestamp with time zone NOT NULL,
						PRIMARY KEY ( id )
					);`,
				},
			},
			{
				DB:          db.DB,
				Description: "Reset node reputations to re-enable disqualification",
				Version:     71,
				Action: migrate.SQL{
					`UPDATE nodes SET audit_reputation_beta = 0;`,
				},
			},
			{
				DB:          db.DB,
				Description: "Add unique to user_credits to match dbx schema",
				Version:     72,
				Action: migrate.SQL{
					`ALTER TABLE user_credits ADD UNIQUE (id, offer_id);`,
				},
			},
			{
				DB:          db.DB,
				Description: "Add node downtime tracking table",
				Version:     73,
				Action: migrate.SQL{
					`CREATE TABLE nodes_offline_times (
						node_id bytea NOT NULL,
						tracked_at timestamp with time zone NOT NULL,
						seconds integer NOT NULL,
						PRIMARY KEY ( node_id, tracked_at )
					);`,
					`CREATE INDEX nodes_offline_times_node_id_index ON nodes_offline_times ( node_id );`,
				},
			},
			{
				DB:          db.DB,
				Description: "Drop storagenode_bandwidth_rollups allocated not null constraint",
				Version:     74,
				Action: migrate.SQL{
					`ALTER TABLE storagenode_bandwidth_rollups ALTER COLUMN allocated DROP NOT NULL;`,
					`ALTER TABLE storagenode_bandwidth_rollups ALTER COLUMN allocated SET DEFAULT 0;`,
				},
			},
			{
				DB:          db.DB,
				Description: "Drop coupon related tables",
				Version:     75,
				Action: migrate.SQL{
					`DROP TABLE coupon_usages;`,
					`DROP TABLE coupons;`,
				},
			},
			{
				DB:          db.DB,
				Description: "Update coupon related tables",
				Version:     76,
				Action: migrate.SQL{
					`CREATE TABLE coupons (
						id bytea NOT NULL,
						project_id bytea NOT NULL,
						user_id bytea NOT NULL,
						amount bigint NOT NULL,
						description text NOT NULL,
						type integer NOT NULL,
						status integer NOT NULL,
						duration bigint NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( id )
					);`,
					`CREATE TABLE coupon_usages (
						coupon_id bytea NOT NULL,
						amount bigint NOT NULL,
						status integer NOT NULL,
						period timestamp with time zone NOT NULL,
						PRIMARY KEY ( coupon_id, period )
					);`,
				},
			},
			{
				DB:          db.DB,
				Description: "Create reported_serials table for faster order processing",
				Version:     77,
				Action: migrate.SQL{
					`CREATE TABLE reported_serials (
						expires_at timestamp with time zone NOT NULL,
						storage_node_id bytea NOT NULL,
						bucket_id bytea NOT NULL,
						action integer NOT NULL,
						serial_number bytea NOT NULL,
						settled bigint NOT NULL,
						observed_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( expires_at, storage_node_id, bucket_id, action, serial_number )
					);`,
				},
			},
			{
				DB:          db.DB,
				Description: "Drop unused indexes",
				Version:     78,
				Action: migrate.SQL{
					`DROP INDEX bucket_name_project_id_interval_start_interval_seconds;`,
					`DROP INDEX storagenode_id_interval_start_interval_seconds_index;`,
				},
			},
			{
				DB:          db.DB,
				Description: "Migrate transactions adding new status completed",
				Version:     79,
				Action: migrate.SQL{
					// delete all pending apply balance intents
					`DELETE FROM stripecoinpayments_apply_balance_intents WHERE state = 0`,
					// create apply balance intents for all misinterpreted transaction
					`INSERT INTO stripecoinpayments_apply_balance_intents
						(SELECT id, 0, now() FROM coinpayments_transactions WHERE status = 100)`,
					// update all received transactions with applied balance intent to be completed
					`UPDATE coinpayments_transactions AS txs
						SET status = 100
						FROM stripecoinpayments_apply_balance_intents AS ints
						WHERE ints.tx_id = txs.id
						AND txs.status = 1
						AND ints.state = 1
					`,
				},
			},
			{
				DB:          db.DB,
				Description: "Add rate_limit column to projects table",
				Version:     80,
				Action: migrate.SQL{
					`ALTER TABLE projects ADD COLUMN rate_limit integer;`,
				},
			},
			{
				DB:          db.DB,
				Description: "Create Credits related tables",
				Version:     81,
				Action: migrate.SQL{
					`CREATE TABLE credits (
						user_id bytea NOT NULL,
						transaction_id text NOT NULL,
						amount bigint NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( transaction_id )
					);`,
					`CREATE TABLE spendings (
						id bytea NOT NULL,
						user_id bytea NOT NULL,
						project_id bytea NOT NULL,
						amount bigint NOT NULL,
						status int NOT NULL,
						created_at timestamp with time zone NOT NULL,
						PRIMARY KEY ( id )
					);`,
				},
			},
		},
	}
}
