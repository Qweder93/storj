// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"storj.io/storj/cmd/uplink/cmd"
	"storj.io/storj/pkg/process"
)

func main() {
	process.ExecWithCustomConfig(cmd.RootCmd, func(cmd *cobra.Command, vip *viper.Viper) error {
		accessFlag := cmd.Flags().Lookup("access")
		// try to load configuration because we may still need 'accesses' (for named access)
		// field but error only if 'access' flag is not set
		err := process.LoadConfig(cmd, vip)
		if err != nil && (accessFlag == nil || accessFlag.Value.String() == "") {
			return err
		}
		return nil
	})
}
