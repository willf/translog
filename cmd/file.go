package cmd

import (
	"github.com/spf13/cobra"
	"github.com/willf/translog/run"
	"github.com/willf/translog/worker"
)

// fileCmd represents the file command
var fileCmd = &cobra.Command{
	Use:   "file",
	Short: "send log data to a file",
	Long:  `Send log data to another file in JSONL format`,
	Run: func(cmd *cobra.Command, args []string) {
		w := &worker.FileWorker{}
		run.Run(w)
	},
}

func init() {
	RootCmd.AddCommand(fileCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// fileCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// fileCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
