package cmd

import (
	"github.com/spf13/cobra"
	"github.com/willf/translog/run"
	"github.com/willf/translog/worker"
)

// stdoutCmd represents the stdout command
var stdoutCmd = &cobra.Command{
	Use:   "stdout",
	Short: "send log data to stdout",
	Long:  `Send log data to stdout`,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Work your own magic here
		w := &worker.StdOutWorker{}
		run.Run(w)
		// go run(worker worker.Worker, config *viper.Viper)
	},
}

func init() {
	RootCmd.AddCommand(stdoutCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// stdoutCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// stdoutCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
