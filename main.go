package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"unicode"

	simplejson "github.com/bitly/go-simplejson"
	pg_query "github.com/lfittl/pg_query_go"
	"github.com/tidwall/gjson"
)

func str2Map(jsonData string) (result map[string]interface{}, err error) {
	err = json.Unmarshal([]byte(jsonData), &result)
	fmt.Println(result)
	return
}

func main() {
	sql, err := ioutil.ReadFile("request.sql")
	if err != nil {
		fmt.Println("File reading error", err)
		return
	}
	fmt.Println("request of sql:\n", string(sql))
	tree, err := pg_query.ParseToJSON(string(sql))
	if err != nil {
		panic(err)
	}

	var m []map[string]interface{}
	err1 := json.Unmarshal([]byte(tree), &m)
	if nil != err1 {
		fmt.Println(err1)
		return
	}

	for _, v1 := range m {
		RawStmtmap := v1["RawStmt"]
		stmt, err := json.Marshal(RawStmtmap)
		if err != nil {
			fmt.Println("json.Marshal failed:", err)
			return
		}

		var stmtstr string = string(stmt[:])
		tablename := gjson.Get(stmtstr, "stmt.CreateStmt.relation.RangeVar.relname").String()

		for _, r := range tablename {
			if unicode.IsUpper(r) {
				fmt.Println("表名", tablename, "请以小写字母\n")
				break
			}
		}
		tablesrtarray := [100]rune{}

		for k1, r1 := range tablename {
			tablesrtarray[k1] = r1
		}

		const rune1 rune = 112
		const rune2 rune = 103

		res, err := simplejson.NewJson([]byte(stmtstr))
		if err != nil {
			fmt.Printf("%v\n", err)
			return
		}

		rows, err := res.Get("stmt").Get("CreateStmt").Get("tableElts").Array()
		var ColumnDefjson []byte
		var Contype int64
		var ColumnDef interface{}
		var Columnname string
		Columnnameslice := make([]string, 100)
		var iskey bool
		slice := make([]int64, 100)

		for _, ColumnDef = range rows {
			ColumnDefjson, err = json.Marshal(ColumnDef)
			if err != nil {
				fmt.Println("json.Marshal failed:", err)
				return
			}

			var ColumnDefstr string = string(ColumnDefjson[:])

			Columndetailmap, err := simplejson.NewJson([]byte(ColumnDefstr))
			if err != nil {
				fmt.Printf("%v\n", err)
				return
			}

			Columndetail, err := Columndetailmap.Get("ColumnDef").Get("typeName").Get("TypeName").Get("names").Array()
			Constraintsdetail, err := Columndetailmap.Get("ColumnDef").Get("constraints").Array()
			for _, Constraintsall := range Constraintsdetail {
				Constraintjson, err := json.Marshal(Constraintsall)
				if err != nil {
					fmt.Println("json.Marshal failed:", err)
				}

				var Constraintsstr string = string(Constraintjson[:])
				Contype = gjson.Get(Constraintsstr, "Constraint.contype").Int()
				slice = append(slice, Contype)

			}

			if tablesrtarray[0] == rune1 && tablesrtarray[1] == rune2 {
				fmt.Println("tablename:", tablename, "不建议以pg开头\n")
			} else {
				for _, slicenum := range slice {
					if slicenum == 5 {
						iskey = true
						break
					}
				}
			}

			var Columntypejson []byte
			for _, Columntypeall := range Columndetail {
				Columntypejson, err = json.Marshal(Columntypeall)
				if err != nil {
					fmt.Println("json.Marshal failed:", err)
				}
			}
			var Columntypestr string = string(Columntypejson[:])
			Columnname = gjson.Get(ColumnDefstr, "ColumnDef.colname").String()
			Columnnameslice = append(Columnnameslice, Columnname)

			Columntype := gjson.Get(Columntypestr, "String.str").String()
			const noticetype1 string = "json"
			const noticetype2 string = "timestamp"
			const noticetype3 string = "text"

			//for _, keynamevalue := range keyname {
			//	if keynamevalue == Columnname {
			//		fmt.Println(Columnname)
			//	} else {
			//		fmt.Println("此列名不是关键字", Columnname)
			//	}
			//}

			if Columntype == noticetype1 {
				fmt.Println(Columnname, "该字段类型请替换为jsonb\n")
			} else if Columntype == noticetype2 {
				fmt.Println(Columnname, "该字段类型请替换为timestamptz\n")
			} else if Columntype == noticetype3 {
				fmt.Println(Columnname, "该字段类型请替换为varchar(n)\n")
			}
			for _, tablenamestr := range Columnname {
				if unicode.IsUpper(tablenamestr) {
					fmt.Println("列名", Columnname, "请以小写字母\n")
					break
				}
			}
		}

		keyname := [442]string{"abort", "absolute", "access", "action", "add", "admin", "after", "aggregate", "all", "also", "alter", "always", "analyse", "analyze", "and", "any", "array", "as", "asc", "assertion", "assignment", "asymmetric", "at", "attach", "attribute", "authorization", "backward", "before", "begin", "between", "bigint", "binary", "bit", "boolean", "both", "by", "cache", "call", "called", "cascade", "cascaded", "case", "cast", "catalog", "chain", "char", "character", "characteristics", "check", "checkpoint", "class", "close", "cluster", "coalesce", "collate", "collation", "column", "columns", "comment", "comments", "commit", "committed", "concurrently", "configuration", "conflict", "connection", "constraint", "constraints", "content", "continue", "conversion", "copy", "cost", "create", "cross", "csv", "cube", "current", "current_catalog", "current_date", "current_role", "current_schema", "current_time", "current_timestamp", "current_user", "cursor", "cycle", "data", "database", "day", "deallocate", "dec", "decimal", "declare", "default", "defaults", "deferrable", "deferred", "definer", "delete", "delimiter", "delimiters", "depends", "desc", "detach", "dictionary", "disable", "discard", "distinct", "do", "document", "domain", "double", "drop", "each", "else", "enable", "encoding", "encrypted", "end", "enum", "escape", "event", "except", "exclude", "excluding", "exclusive", "execute", "exists", "explain", "extension", "external", "extract", "false", "family", "fetch", "filter", "first", "float", "following", "for", "force", "foreign", "forward", "freeze", "from", "full", "function", "functions", "generated", "global", "grant", "granted", "greatest", "group", "grouping", "groups", "handler", "having", "header", "hold", "hour", "identity", "if", "ilike", "immediate", "immutable", "implicit", "import", "in", "include", "including", "increment", "index", "indexes", "inherit", "inherits", "initially", "inline", "inner", "inout", "input", "insensitive", "insert", "instead", "int", "integer", "intersect", "interval", "into", "invoker", "is", "isnull", "isolation", "join", "key", "label", "language", "large", "last", "lateral", "leading", "leakproof", "least", "left", "level", "like", "limit", "listen", "load", "local", "localtime", "localtimestamp", "location", "lock", "locked", "logged", "mapping", "match", "materialized", "maxvalue", "method", "minute", "minvalue", "mode", "month", "move", "name", "names", "national", "natural", "nchar", "new", "next", "no", "none", "not", "nothing", "notify", "notnull", "nowait", "null", "nullif", "nulls", "numeric", "object", "of", "off", "offset", "oids", "old", "on", "only", "operator", "option", "options", "or", "order", "ordinality", "others", "out", "outer", "over", "overlaps", "overlay", "overriding", "owned", "owner", "parallel", "parser", "partial", "partition", "passing", "password", "placing", "plans", "policy", "position", "preceding", "precision", "prepare", "prepared", "preserve", "primary", "prior", "privileges", "procedural", "procedure", "procedures", "program", "publication", "quote", "range", "read", "real", "reassign", "recheck", "recursive", "ref", "references", "referencing", "refresh", "reindex", "relative", "release", "rename", "repeatable", "replace", "replica", "reset", "restart", "restrict", "returning", "returns", "revoke", "right", "role", "rollback", "rollup", "routine", "routines", "row", "rows", "rule", "savepoint", "schema", "schemas", "scroll", "search", "second", "security", "select", "sequence", "sequences", "serializable", "server", "session", "session_user", "set", "setof", "sets", "share", "show", "similar", "simple", "skip", "smallint", "snapshot", "some", "sql", "stable", "standalone", "start", "statement", "statistics", "stdin", "stdout", "storage", "stored", "strict", "strip", "subscription", "substring", "support", "symmetric", "sysid", "system", "table", "tables", "tablesample", "tablespace", "temp", "template", "temporary", "text", "then", "ties", "time", "timestamp", "to", "trailing", "transaction", "transform", "treat", "trigger", "trim", "true", "truncate", "trusted", "type", "types", "unbounded", "uncommitted", "unencrypted", "union", "unique", "unknown", "unlisten", "unlogged", "until", "update", "user", "using", "vacuum", "valid", "validate", "validator", "value", "values", "varchar", "variadic", "varying", "verbose", "version", "view", "views", "volatile", "when", "where", "whitespace", "window", "with", "within", "without", "work", "wrapper", "write", "xml", "xmlattributes", "xmlconcat", "xmlelement", "xmlexists", "xmlforest", "xmlnamespaces", "xmlparse", "xmlpi", "xmlroot", "xmlserialize", "xmltable", "year", "yes", "zone"}
		for _, Columnnamevalue := range Columnnameslice {
			for _, keynamevalue := range keyname {
				if Columnnamevalue == keynamevalue {
					fmt.Println(Columnnamevalue, "与sql关键字冲突建议替换\n")
				} else {
					break
				}
			}
		}

		if iskey == false {
			fmt.Println(tablename, "表不存在主键\n")
		} else {
			break
		}

	}
}
