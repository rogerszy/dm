// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package loader

import (
	"database/sql"
	"fmt"
	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

// Conn represents a live DB connection
type Conn struct {
	cfg *config.SubTaskConfig

	db *sql.DB
}

func (conn *Conn) querySQL(ctx *tcontext.Context, query string, maxRetry int, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.db == nil {
		return nil, errors.NotValidf("database connection")
	}

	var (
		err  error
		rows *sql.Rows
	)

	ctx.L().Debug("query statement", zap.String("sql", query), zap.Reflect("arguments", args))
	startTime := time.Now()
	defer func() {
		if err == nil {
			cost := time.Since(startTime)
			queryHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
			if cost > 1 {
				ctx.L().Warn("query statement", zap.String("sql", query), zap.Reflect("arguments", args), zap.Duration("cost time", cost))
			}
		}
	}()

	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second)
			ctx.L().Warn("query statement", zap.Int("retry", i), zap.String("sql", query), zap.Reflect("arguments", args))
		}
		rows, err = conn.db.QueryContext(ctx.Context(), query, args...)
		if err != nil {
			if isRetryableError(err) {
				ctx.L().Warn("query statement", zap.Int("retry", i), zap.String("sql", query), zap.Reflect("arguments", args), log.ShortError(err))
				continue
			}
			return nil, errors.Trace(err)
		}
		return rows, nil
	}
	return nil, errors.Annotatef(err, "query statement, sql [%s] args [%v] failed", query, args)
}

func (conn *Conn) executeSQL2(ctx *tcontext.Context, stmt string, maxRetry int, args ...interface{}) error {
	if conn == nil || conn.db == nil {
		return errors.NotValidf("database connection")
	}

	var err error
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			ctx.L().Warn("execute statement", zap.Int("retry", i), zap.String("sql", stmt), zap.Reflect("arguments", args))
			time.Sleep(time.Duration(i) * time.Second)
		}
		_, err = conn.db.ExecContext(ctx.Context(), stmt, args...)
		if err != nil {
			if isRetryableError(err) {
				ctx.L().Warn("execute statement", zap.Int("retry", i), zap.String("sql", stmt), zap.Reflect("arguments", args), log.ShortError(err))
				continue
			}
			return errors.Trace(err)
		}
		return nil
	}
	return errors.Annotatef(err, "exec sql[%s] args[%v] failed", stmt, args)
}

func (conn *Conn) executeSQL(ctx *tcontext.Context, sqls []string, enableRetry bool) error {
	return conn.executeSQLCustomRetry(ctx, sqls, enableRetry, isRetryableError)
}

func (conn *Conn) executeDDL(ctx *tcontext.Context, sqls []string, enableRetry bool) error {
	return conn.executeSQLCustomRetry(ctx, sqls, enableRetry, isDDLRetryableError)
}

func (conn *Conn) executeSQLCustomRetry(ctx *tcontext.Context, sqls []string, enableRetry bool, isRetryableFn func(err error) bool) error {
	if len(sqls) == 0 {
		return nil
	}

	if conn == nil || conn.db == nil {
		return errors.NotValidf("database connection")
	}

	var err error
	dupErrFlag := false

	retryCount := 1
	if enableRetry {
		retryCount = maxRetryCount
	}

	for i := 0; i < retryCount; i++ {
		if i > 0 {
			ctx.L().Debug("execute statement", zap.Int("retry", i), zap.String("sqls", fmt.Sprintf("%-.200v", sqls)))
			time.Sleep(2 * time.Duration(i) * time.Second)
		}

		startTime := time.Now()
		err = executeSQLImp(ctx, conn.db, sqls)
		if err != nil {
			tidbExecutionErrorCounter.WithLabelValues(conn.cfg.Name).Inc()
			dupErr := errors.Cause(err) // Modify the SQL and try again for ErrDupEntry
			if isMySQLError(dupErr, tmysql.ErrDupEntry) {
				newSQL, modErr := modifySQL(conn.db, err, sqls[1])
				if modErr != nil {
					ctx.L().Info("execute statement", zap.String("modErr", fmt.Sprint(modErr)))
					return errors.Trace(err)
				}
				sqls[1] = newSQL
				i--
				dupErrFlag = true
				continue
			} else if isMySQLError(dupErr, tmysql.ErrParse) && dupErrFlag {
				ctx.L().Warn("execute statement", zap.String("Modified", "All data is dupError"))
				return nil // All data is dupError
			}
			if isRetryableFn(err) {
				continue
			}
			return errors.Trace(err)
		}

		// update metrics
		cost := time.Since(startTime)
		txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
		if cost > 1 {
			ctx.L().Warn("transaction execute successfully", zap.Duration("cost time", cost))
		}

		return nil
	}

	return errors.Trace(err)
}

func executeSQLImp(ctx *tcontext.Context, db *sql.DB, sqls []string) error {
	var (
		err error
		txn *sql.Tx
		res sql.Result
	)

	txn, err = db.Begin()
	if err != nil {
		ctx.L().Error("fail to begin a transaction", zap.String("sqls", fmt.Sprintf("%-.200v", sqls)), zap.Error(err))
		return err
	}

	for i := range sqls {
		ctx.L().Debug("execute statement", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])))
		res, err = txn.ExecContext(ctx.Context(), sqls[i])
		if err != nil {
			ctx.L().Warn("execute statement", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])), log.ShortError(err))
			rerr := txn.Rollback()
			if rerr != nil {
				ctx.L().Error("fail rollback", log.ShortError(rerr))
			}
			return err
		}
		// check update checkpoint successful or not
		if i == 2 {
			row, err1 := res.RowsAffected()
			if err1 != nil {
				ctx.L().Warn("fail to get rows affected", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])), log.ShortError(err1))
				continue
			}
			if row != 1 {
				ctx.L().Warn("update checkpoint", zap.Int64("affected rows", row))
			}
		}
	}

	err = txn.Commit()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func createConn(cfg *config.SubTaskConfig) (*Conn, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&maxAllowedPacket=%d",
		cfg.To.User, cfg.To.Password, cfg.To.Host, cfg.To.Port, *cfg.To.MaxAllowedPacket)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Conn{db: db, cfg: cfg}, nil
}

func closeConn(conn *Conn) error {
	if conn.db == nil {
		return nil
	}

	return errors.Trace(conn.db.Close())
}

func isErrDBExists(err error) bool {
	return isMySQLError(err, tmysql.ErrDBCreateExists)
}

func isErrTableExists(err error) bool {
	return isMySQLError(err, tmysql.ErrTableExists)
}

func isErrDupEntry(err error) bool {
	return isMySQLError(err, tmysql.ErrDupEntry)
}

func isRetryableError(err error) bool {
	err = errors.Cause(err)
	if isMySQLError(err, tmysql.ErrDupEntry) {
		return false
	}
	if isMySQLError(err, tmysql.ErrDataTooLong) {
		return false
	}
	return true
}

func isDDLRetryableError(err error) bool {
	if isErrTableExists(err) || isErrDBExists(err) {
		return false
	}
	return isRetryableError(err)
}

func isMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

func modifySQL(db *sql.DB, err error, sql string) (string, error) {
	dupData := ""
	var table string
	reg := regexp.MustCompile(`^INSERT INTO .*?VALUES`)
	if reg.MatchString(sql) {
		tmpTable := reg.FindAllString(sql, 1)
		table = tmpTable[0][13 : len(tmpTable[0])-8]
	} else {
		return sql, errors.New("Can't get table name")
	}

	tmpDupData, keyName, getErr := getDupEntry(err) // Get duplicate data and index key name
	if getErr != nil {
		return sql, getErr
	}
	// The ' and " in the dupData obtained from the error message has no escape character \ , so the escape character must be added after obtaining dupData
	for i := 0; i < len(tmpDupData); i++ {
		if tmpDupData[i] == '\'' || tmpDupData[i] == '"' || tmpDupData[i] == '\\' {
			dupData += "\\" + string(tmpDupData[i])
		} else {
			dupData += string(tmpDupData[i])
		}
	}

	rows, err := db.Query("SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` WHERE table_name='" + table + "' AND constraint_name=" + keyName)

	if err != nil {
		return sql, err
	}

	var columnName string
	for rows.Next() {
		err := rows.Scan(&columnName)
		if err != nil {
			return sql, err
		}
	}

	rows, err = db.Query("SELECT column_name FROM information_schema.columns WHERE table_name='" + table + "';")
	if err != nil {
		return sql, err
	}
	pos := 1

	for rows.Next() {
		var tmpColumnName string
		err := rows.Scan(&tmpColumnName)
		if err != nil {
			return sql, err
		}
		if tmpColumnName == columnName {
			break
		}
		pos++
	}

	newSQL, errorData, delErr := delDupEntry(sql, dupData, pos) // Delete duplicate data pair
	if delErr != nil {
		return sql, delErr
	}

	logErr := errorLog(errorData, table, dupData, columnName, strconv.Itoa(pos)) // Write dupdata to the errorlog
	if logErr != nil {
		return sql, logErr
	}
	return newSQL, nil
}

func getDupEntry(err error) (string, string, error) {
	var keyName string
	if len(fmt.Sprint(err)) < 40 {
		return "", "", errors.New("Incorrect error information")
	}
	reg := regexp.MustCompile(`for key '[^ ]*'`)
	tmpKeyName := reg.FindAllString(fmt.Sprint(err), -1)

	if reg.MatchString(fmt.Sprint(err)) {
		keyName = tmpKeyName[len(tmpKeyName)-1][8:]
	} else {
		return "", "", errors.New("Can't find dupEntry key")
	}
	dupData := fmt.Sprint(err)[29 : len(fmt.Sprint(err))-len(tmpKeyName[len(tmpKeyName)-1])-2]
	return dupData, keyName, nil
}

func delDupEntry(target, dupData string, pos int) (string, string, error) {
	var trxStart int
	var delRes string      // SQL statement after deduplication
	var dupDataPair string // Duplicate data pair
	trxStart = 0
	dataIn := false // Use a bool type to record whether the current data ends or not, avoiding encountering ( and ) in the data that causes the current data pair to be considered finished or restarted
	dataNow := 0    // Record the start of each match
	nowPos := 0     // Record the location of the current data, and compare whether the data is equal to dupData only when it is equal to pos
	isSymbol := 0   // Records whether the current data carries ', Used to record data locations and compare data, 0 for stateless, 1 for currently traversing string data, 2 for currently traversing non-string data
	isFind := false // Use a variable to record whether duplicate values have been found
	for i := 0; i < len(target); i++ {
		switch target[i] {
		case '(':
			if dataIn == false {
				trxStart = i             // Left edge position of deleted data
				if target[i+1] != '\'' { // For non-string data after (
					nowPos++
					dataNow = i
					isSymbol = 2
				}
			}
		case '\\': // When an escape character is encountered, need to skip the next character to avoid the effect of characters such as ' "
			i++
			continue
		case '\'':
			if dataIn == false {
				dataIn = true
				isSymbol = 1
				nowPos++
				dataNow = i
			} else {
				dataIn = false
				isSymbol = 0
				if nowPos == pos { // Find a string data
					if target[dataNow+1:i] == dupData {
						isFind = true
					}
				}
			}
		case ',':
			if isSymbol == 0 && dataIn == false && target[i-1] != ')' && target[i+1] != '\'' { // No "," can appear before the beginning of non-string data, and cannot between the string data, but between the "(" and ")", and the last bit cannot be a "'"
				nowPos++
				dataNow = i
				isSymbol = 2
			} else if isSymbol == 2 {
				isSymbol = 0
				if nowPos == pos { // Find a non-string data
					if target[dataNow+1:i] == dupData {
						isFind = true
					}
				}
				if target[i+1] != '\'' { // The "," may be the beginning of another data at the same time as the end of the data
					nowPos++
					dataNow = i
					isSymbol = 2
				}
			}
		case ')':
			if dataIn == false {
				if isSymbol == 2 { // For non-string data before )
					isSymbol = 0
					if nowPos == pos { // Find a non-string data
						if target[dataNow+1:i] == dupData {
							isFind = true
						}
					}
				}
				nowPos = 0
				if isFind == true {
					if target[i+1] == ';' { // If the next digit is ";", delete the "," before the data
						tmpSymbol := 2 // Find a "," or "S" that before the duplicate data pair
						for target[trxStart-tmpSymbol] != ',' && target[trxStart-tmpSymbol] != 'S' {
							tmpSymbol++
						}
						delRes = target[:trxStart-tmpSymbol] + target[i+1:]
					} else {
						delRes = target[:trxStart] + target[i+2:]
					}
					dupDataPair = target[trxStart : i+1]
					return delRes, dupDataPair, nil
				}
			}
		}
	}
	return delRes, "", errors.New("Can't find dup Error")
}

func errorLog(dupDataSQL, errorTable, errorData, errorKey, errorPos string) error {
	path := "./dupDataFile.txt"
	var dupDataFile *os.File
	_, err := os.Stat(path) // Detects if the file exists
	if os.IsNotExist(err) {
		dupDataFile, err = os.Create(path)
		if err != nil {
			return err
		}
	} else {
		dupDataFile, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}
	}
	_, err = dupDataFile.WriteString("*********************\n" + "Key: " + errorKey + "\n" + "Data: " + errorData + "\n" + "Pos: " + errorPos + "\n")
	if err != nil {
		closeErr := dupDataFile.Close()
		if closeErr != nil {
			return closeErr
		}
		return err
	}
	_, err = dupDataFile.WriteString("INSERT INTO `" + errorTable + "` VALUES" + dupDataSQL + ";\n")
	if err != nil {
		closeErr := dupDataFile.Close()
		if closeErr != nil {
			return closeErr
		}
		return err
	}
	err = dupDataFile.Close()
	if err != nil {
		return err
	}
	return nil
}
