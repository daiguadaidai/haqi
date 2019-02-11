// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"os"

	"github.com/daiguadaidai/haqi/config"
	"github.com/daiguadaidai/haqi/services/manal"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "haqi",
	Short: "MySQL binglog 解析应用工具",
}

// cerateCmd 是 rootCmd 的一个子命令
var manalCmd = &cobra.Command{
	Use:   "tomysql",
	Short: "将并log应用到mysql",
	Long: `将指定的binglog应用到mysql
Example:
指定 开始位点 和 结束位点
./haqi tomysql \
    --start-log-file="mysql-bin.000090" \
    --start-log-pos=0 \
    --end-log-file="mysql-bin.000092" \
    --end-log-pos=424 \
    --thread-id=15 \
    --trans-schema="schema1" \
    --trans-table="schema2.table1" \
    --enable-trans-insert=false \
    --enable-trans-update=false \
    --enable-trans-delete=true \
    --ori-db-host="127.0.0.1" \
    --ori-db-port=3306 \
    --ori-db-username="root" \
    --ori-db-password="root" \
    --std-db-host="127.0.0.1" \
    --std-db-port=3306 \
    --std-db-username="root" \
    --std-db-password="root" \
    --task-uuid="201901182256351181056356ymnuqk" \
    --read-api="http://127.0.0.1:19528/api/v1/pili/tasks/get" \
    --update-api="http://127.0.0.1:19528/api/v1/pili/tasks"
`,
	Run: func(cmd *cobra.Command, args []string) {
		manal.Start(manalTMC, manalODBC, manalTDBC)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	addManalCMD()
}

var manalTMC *config.ToMySQLConfig
var manalODBC *config.DBConfig // 源数据库配置信息
var manalTDBC *config.DBConfig // 目标数据库配置信息

// 添加创建回滚SQL子命令
func addManalCMD() {
	rootCmd.AddCommand(manalCmd)
	manalTMC = new(config.ToMySQLConfig)
	manalCmd.PersistentFlags().StringVar(&manalTMC.StartLogFile, "start-log-file",
		"", "开始日志文件")
	manalCmd.PersistentFlags().Uint32Var(&manalTMC.StartLogPos, "start-log-pos",
		0, "开始日志文件点位")
	manalCmd.PersistentFlags().StringVar(&manalTMC.EndLogFile, "end-log-file",
		"", "结束日志文件")
	manalCmd.PersistentFlags().Uint32Var(&manalTMC.EndLogPos, "end-log-pos",
		0, "结束日志文件点位")
	manalCmd.PersistentFlags().StringSliceVar(&manalTMC.TransSchemas, "trans-schema",
		make([]string, 0, 1), "指定需要执行的schema, 该命令可以指定多个")
	manalCmd.PersistentFlags().StringSliceVar(&manalTMC.TransTables, "trans-table",
		make([]string, 0, 1), "需要执行的表, 该命令可以指定多个")
	manalCmd.PersistentFlags().Uint32Var(&manalTMC.ThreadID, "thread-id",
		0, "需要执行的thread id")
	manalCmd.PersistentFlags().BoolVar(&manalTMC.EnableTransInsert, "enable-trans-insert",
		config.ENABLE_TRANS_INSERT, "是否启用执行 insert")
	manalCmd.PersistentFlags().BoolVar(&manalTMC.EnableTransUpdate, "enable-trans-update",
		config.ENABLE_TRANS_UPDATE, "是否启用执行 update")
	manalCmd.PersistentFlags().BoolVar(&manalTMC.EnableTransDelete, "enable-trans-delete",
		config.ENABLE_TRANS_DELETE, "是否启用执行 delete")
	manalCmd.PersistentFlags().StringVar(&manalTMC.TaskUUID, "task-uuid",
		"", "关联的任务UUID")
	manalCmd.PersistentFlags().StringVar(&manalTMC.UpdateAPI, "update-api",
		"", "更新任务信息API")
	manalCmd.PersistentFlags().StringVar(&manalTMC.ReadAPI, "read-api",
		"", "获取任务信息API")

	// 源链接的数据库配置
	manalODBC = new(config.DBConfig)
	manalCmd.PersistentFlags().StringVar(&manalODBC.Host, "ori-db-host",
		config.DB_HOST, "(源)数据库host")
	manalCmd.PersistentFlags().IntVar(&manalODBC.Port, "ori-db-port",
		config.DB_PORT, "(源)数据库port")
	manalCmd.PersistentFlags().StringVar(&manalODBC.Username, "ori-db-username",
		config.DB_USERNAME, "(源)数据库用户名")
	manalCmd.PersistentFlags().StringVar(&manalODBC.Password, "ori-db-password",
		config.DB_PASSWORD, "(源)数据库密码")
	manalCmd.PersistentFlags().StringVar(&manalODBC.Database, "ori-db-schema",
		config.DB_SCHEMA, "(源)数据库名称")
	manalCmd.PersistentFlags().StringVar(&manalODBC.CharSet, "ori-db-charset",
		config.DB_CHARSET, "(源)数据库字符集")
	manalCmd.PersistentFlags().IntVar(&manalODBC.Timeout, "ori-db-timeout",
		config.DB_TIMEOUT, "(源)数据库timeout")
	manalCmd.PersistentFlags().IntVar(&manalODBC.MaxIdelConns, "ori-db-max-idel-conns",
		config.DB_MAX_IDEL_CONNS, "(源)数据库最大空闲连接数")
	manalCmd.PersistentFlags().IntVar(&manalODBC.MaxOpenConns, "ori-db-max-open-conns",
		config.DB_MAX_OPEN_CONNS, "(源)数据库最大连接数")
	manalCmd.PersistentFlags().BoolVar(&manalODBC.AutoCommit, "ori-db-auto-commit",
		config.DB_AUTO_COMMIT, "(源)数据库自动提交")

	// 目标链接的数据库配置
	manalTDBC = new(config.DBConfig)
	manalCmd.PersistentFlags().StringVar(&manalTDBC.Host, "std-db-host",
		config.DB_HOST, "(目标)数据库host")
	manalCmd.PersistentFlags().IntVar(&manalTDBC.Port, "std-db-port",
		config.DB_PORT, "(目标)数据库port")
	manalCmd.PersistentFlags().StringVar(&manalTDBC.Username, "std-db-username",
		config.DB_USERNAME, "(目标)数据库用户名")
	manalCmd.PersistentFlags().StringVar(&manalTDBC.Password, "std-db-password",
		config.DB_PASSWORD, "(目标)数据库密码")
	manalCmd.PersistentFlags().StringVar(&manalTDBC.Database, "std-db-schema",
		config.DB_SCHEMA, "(目标)数据库名称")
	manalCmd.PersistentFlags().StringVar(&manalTDBC.CharSet, "std-db-charset",
		config.DB_CHARSET, "(目标)数据库字符集")
	manalCmd.PersistentFlags().IntVar(&manalTDBC.Timeout, "std-db-timeout",
		config.DB_TIMEOUT, "(目标)数据库timeout")
	manalCmd.PersistentFlags().IntVar(&manalTDBC.MaxIdelConns, "std-db-max-idel-conns",
		config.DB_MAX_IDEL_CONNS, "(目标)数据库最大空闲连接数")
	manalCmd.PersistentFlags().IntVar(&manalTDBC.MaxOpenConns, "std-db-max-open-conns",
		config.DB_MAX_OPEN_CONNS, "(目标)数据库最大连接数")
	manalCmd.PersistentFlags().BoolVar(&manalTDBC.AutoCommit, "std-db-auto-commit",
		config.DB_AUTO_COMMIT, "(目标)数据库自动提交")
}
