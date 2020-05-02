package service

import (
	"fmt"
	protocol "github.com/withlin/canal-go/protocol"
	"github.com/withlin/canal-go/samples/gredis"
)


//shop_id : s1id5ea3d3292adv0  update= false
//open_id : oePKH5DRzZpwkW5YhSZ2cRNNz-f4  update= false
//shop_name :   update= false
//head_url : 111211  update= true
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

//================> binlog[mysql-bin.000008 : 6876],name[game,game_user], eventType: INSERT
//user_id : 12  update= true
//open_id :   update= true
//nick_name :   update= true
//avatar_url :   update= true
//phone :   update= true
//gender : 1  update= true
//type : 1  update= true
//city :   update= true
//province :   update= true
//reg_time :   update= true
//================> binlog[mysql-bin.000008 : 7114],name[game,game_user], eventType: DELETE
//user_id : 12  update= false
//open_id :   update= false
//nick_name :   update= false
//avatar_url :   update= false
//phone :   update= false
//gender : 1  update= false
//type : 1  update= false
//city :   update= false
//province :   update= false
//reg_time :   update= false

func SyncGameUser(tableName string, eventType protocol.EventType, rowDatas []*protocol.RowData) {

	for _, rowData := range rowDatas {
		switch eventType {
		case protocol.EventType_INSERT:
			redisKey := tableName
			var m = make(map[string]string)
			for _, col := range rowData.GetAfterColumns() {
				fmt.Println(fmt.Sprintf("%s : %s  update= %t", col.GetName(), col.GetValue(), col.GetUpdated()))
				if col.GetIsKey() {
					redisKey += ":" + col.GetValue()
				}
				m[col.GetName()] = col.GetValue()
			}
			_, _ = gredis.HMSet(redisKey, m)

		case protocol.EventType_UPDATE:
			redisKey := tableName
			var m = make(map[string]string)
			for _, col := range rowData.GetAfterColumns() {
				if col.GetIsKey() {
					redisKey += ":" + col.GetValue()
				}
				if col.GetUpdated() {
					m[col.GetName()] = col.GetValue()
				}
			}
			_, _ = gredis.HMSet(redisKey, m)
		case protocol.EventType_DELETE:
			redisKey := tableName
			for _, col := range rowData.GetBeforeColumns() {
				fmt.Println(fmt.Sprintf("%s : %s  update= %t", redisKey, col.GetValue(), col.GetUpdated()))
				if col.GetIsKey() {
					redisKey += ":" + col.GetValue()
					fmt.Println(fmt.Sprintf("%s : %s  update2= %t", redisKey, col.GetValue(), col.GetUpdated()))
					break
				}
			}
			_, _ = gredis.Delete(redisKey)
		}
	}


}