// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"github.com/withlin/canal-go/samples/service"
	"log"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/withlin/canal-go/client"
	protocol "github.com/withlin/canal-go/protocol"
)

func main() {

	// 192.168.199.17 替换成你的canal server的地址
	// example 替换成-e canal.destinations=example 你自己定义的名字
	connector := client.NewSimpleCanalConnector("127.0.0.1", 11111, "", "", "example", 60000, 60*60*1000)
	err := connector.Connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// https://github.com/alibaba/canal/wiki/AdminGuide
	//mysql 数据解析关注的表，Perl正则表达式.
	//
	//多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)
	//
	//常见例子：
	//
	//  1.  所有表：.*   or  .*\\..*
	//	2.  canal schema下所有表： canal\\..*
	//	3.  canal下的以canal打头的表：canal\\.canal.*
	//	4.  canal schema下的一张表：canal\\.test1
	//  5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)

	err = connector.Subscribe(".*\\..*")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	for {

		message, err := connector.Get(100, nil, nil)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(300 * time.Millisecond)
			fmt.Println("===没有数据了===")
			continue
		}
		printEntry(message.Entries)

	}
}


//================> binlog[mysql-bin.000008 : 2910],name[global,shop_infos], eventType: UPDATE
//-------> before
//shop_id : s1id5ea3d3292adv0  update= false
//open_id : oePKH5DRzZpwkW5YhSZ2cRNNz-f4  update= false
//shop_name :   update= false
//head_url : 11121  update= false
//phone :   update= false
//video_url :   update= false
//menu :   update= false
//titles : ["qq"]  update= false
//is_open : 0  update= false
//open_time :   update= false
//close_time :   update= false
//has_breakfast : 1  update= false
//has_lunch : 0  update= false
//has_dinner : 0  update= false
//has_night_snack : 0  update= false
//post_code : 0  update= false
//address :   update= false
//longitude : 0.000000000  update= false
//latitude : 0.000000000  update= false
//reg_time : 1587794729  update= false
//business_url :   update= false
//license_url :   update= false
//health_url :   update= false
//shop_type : 2  update= false
//shop_type_btc : 1  update= false
//qr_code_pay : 0  update= false
//team_book : 1  update= false
//check_pass : 0  update= false
//sell_count : 0  update= false
//sell_count_today : 0  update= false
//sell_count_yesterday : 0  update= false
//sell_amount : 0.0  update= false
//sell_amount_today : 0.0  update= false
//sell_amount_yesterday : 0.0  update= false
//today_tick : 0  update= false
//-------> after

func printEntry(entrys []protocol.Entry) {

	for _, entry := range entrys {
		if entry.GetEntryType() == protocol.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == protocol.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(protocol.RowChange)

		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		checkError(err)
		eventType := rowChange.GetEventType()
		header := entry.GetHeader()
		fmt.Println(fmt.Sprintf("================> binlog[%s : %d],name[%s,%s], eventType: %s", header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(), header.GetTableName(), header.GetEventType()))
		if header.GetSchemaName() == "game" {
			switch header.GetTableName() {
			case "game_user":
				service.SyncGameUser(header.GetTableName(), eventType, rowChange.GetRowDatas())
			case "":
			default:

			}
		}
	}
}

func printColumn(columns []*protocol.Column) {
	for _, col := range columns {
		fmt.Println(fmt.Sprintf("%s : %s  update= %t", col.GetName(), col.GetValue(), col.GetUpdated()))
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
