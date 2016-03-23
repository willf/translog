package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// gaCmd represents the ga command
var gaCmd = &cobra.Command{
	Use:   "ga",
	Short: "send log data to Google Analytics",
	Long:  `Send log data to Google Analytics`,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Work your own magic here
		fmt.Println("ga called")
	},
}

func init() {
	RootCmd.AddCommand(gaCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// gaCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// gaCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
