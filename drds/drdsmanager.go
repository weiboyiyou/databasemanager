package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"flag"
	"fmt"
	"strconv"
	"sort"
)

func main(){
	dbname:=flag.String("dbname","all","dbname  or all")
	dbdnsname:=flag.String("dbdnsname","**********************","please input the db dns name!!!")
	user:=flag.String("user","**********","please input the db user name!!!")
	password:=flag.String("password","***********","please input the db password!!!")
	querytime:=flag.Int("querytime",0,"please input the time!!!")
	action:=flag.String("action","print","please input action:print or kill???")
	sqltype:=flag.String("sqltype","sui","please input action:s or su or sui or sleep or all")
	flag.Parse()
	dbs := []string{"db1", "db2"}
	if *dbname=="all" {
		for _,value:=range dbs {
			kill_or_print_onedb(*user,value,*password,*dbdnsname,*querytime,*action,*sqltype)
		}
	}else {
		kill_or_print_onedb(*user,*dbname,*password,*dbdnsname,*querytime,*action,*sqltype)
	}
}

func kill_or_print_onedb(user,dbname string,password string,dbdnsname string,querytime int,action string,sqltype string){
	var sqlstr string
	connstr:=user+":"+password+"@tcp("+dbdnsname+":3306)/"+dbname+"?charset=utf8"
	if action=="print"{
		sqlstr="SHOW full processlist where  time >"+strconv.Itoa(querytime)+" and command!='SLEEP';"
	}else if action=="kill" && sqltype=="sui"{
		sqlstr="SHOW full processlist where  time >"+strconv.Itoa(querytime)+" and command!='SLEEP' and (info like '%select%' or info like '%update%' or info like '%insert%');"
	}else if action=="kill" && sqltype=="su" {
		sqlstr="SHOW full processlist where  time >"+strconv.Itoa(querytime)+" and command!='SLEEP' and (info like '%select%' or info like '%update%');"
	}else if action=="kill" && sqltype=="sleep" {
		sqlstr="SHOW full processlist where  time >"+strconv.Itoa(querytime)+" and command='SLEEP';"
	}else if action=="kill" && sqltype=="all"{

	}else {
		sqlstr="SHOW full processlist where  time >"+strconv.Itoa(querytime)+" and command!='SLEEP' and info like '%select%';"
	}
	//sqlstr:="show processlist where command='SLEEP'"
	db_drds, err := sql.Open("mysql", connstr)
	defer db_drds.Close()
	checkErr(err)
	rows, err := db_drds.Query(sqlstr)
	checkErr(err)
	if action=="kill"{
		for rows.Next() {
			var ID int
			var USER sql.NullString
			var HOST sql.NullString
			var DB sql.NullString
			var COMMAND sql.NullString
			var TIME sql.NullInt64
			var STATE sql.NullString
			var INFO sql.NullString
			err = rows.Scan(&ID,&USER,&HOST,&DB,&COMMAND,&TIME,&STATE,&INFO)
			checkErr(err)
			fmt.Printf("database %s kill pid %d.....\n",DB.String,ID)
			_, err := db_drds.Exec("kill "+strconv.Itoa(ID))
			checkErr(err)
			}
	}else {
		fmt.Println(dbname)
		processlist:=[]Process{}
		for rows.Next() {
			var ID int
			var USER sql.NullString
			var HOST sql.NullString
			var DB sql.NullString
			var COMMAND sql.NullString
			var TIME int
			var STATE sql.NullString
			var INFO sql.NullString
			err = rows.Scan(&ID,&USER,&HOST,&DB,&COMMAND,&TIME,&STATE,&INFO)
			checkErr(err)
			//fmt.Printf("TIME:%d,sql:%s",TIME,INFO.String)
			processlist=append(processlist, Process{ID,USER.String,HOST.String,DB.String,COMMAND.String,TIME,STATE.String,INFO.String})
		}
		sort.Sort(ProcessWrapper{processlist, func (p, q *Process) bool {
			return q.TIME < p.TIME    // TIME 递减排序
		}})
		for _, p := range processlist {
			fmt.Printf("%d,%s,%d,%s,%s\n",p.ID,p.USER,p.TIME,p.HOST,p.INFO)
		}
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func checkErrcontinue(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

type Process struct {
	ID int
	USER string
	HOST string
	DB string
	COMMAND string
	TIME int
	STATE string
	INFO string
}

type ProcessWrapper struct {
	processList [] Process
	by func(p, q * Process) bool
}


func (p ProcessWrapper) Swap(i, j int)      { p.processList[i], p.processList[j] = p.processList[j], p.processList[i] }
func (p ProcessWrapper) Len() int           { return len(p.processList) }
func (p ProcessWrapper) Less(i, j int) bool { return p.by(&p.processList[i], &p.processList[j]) }

/*
func getlongquery(){
*/
