package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/fatih/color"
	_ "gopkg.in/goracle.v2"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

func banner() {
	fmt.Println("THIS TOOL IS NOT FOR PRODUCTION USE!!! Only for fun and learn ;)")
	fmt.Println("created by Kamil Stawiarski (@ora600pl kstawiarski@ora-600.pl ora-600.pl)\n")
}

func usage() {
	fmt.Println("./odbv3 -f path_to_a_data_file -b block_size -c user/password@host:port/service [-a user/pass@host:port/+ASM[n]] [-rs (report space only)")
}

var DB, ASM *sql.DB

func connectDb(conn string) {
	db, err := sql.Open("goracle", conn)
	if err != nil {
		log.Panic(err)
	}
	rows, err := db.Query("select 1 from dual")
	if err != nil {
		log.Panic(err)
	}

	check := 0
	if rows.Next() {
		rows.Scan(&check)
	}
	if check == 1 {
		DB = db
	} else {
		log.Panic("Problem with DB connection")
	}
}

func connectAsm(conn string) {
	connSysdba := conn + " as sysdba"
	db, err := sql.Open("goracle", connSysdba)
	if err != nil {
		log.Panic(err)
	}
	rows, err := db.Query("select 1 from dual")
	if err != nil {
		fmt.Println("ASM problem")
		log.Panic(err)
	}

	check := 0
	if rows.Next() {
		rows.Scan(&check)
	}
	if check == 1 {
		ASM = db
	} else {
		log.Panic("Problem with ASM connection")
	}
}

type KCBH struct {
	Type_kcbh   uint8
	Frmt_kcbh   uint8
	Spare1_kcbh uint8
	Spare2_kcbh uint8
	Rdba_kcbh   uint32
	Bas_kcbh    uint32
	Wrp_kcbh    uint16
	Seq_kcbh    uint8
	Flg_kcbh    uint8
	Chkval_kcbh uint16
	Spare3_kcbh uint16
}

type KTBBHITL struct {
	Kxidusn  uint16
	Kxidslt  uint16
	Kxidsqn  uint32
	Kubadba  uint32
	Kubaseq  uint16
	Kubarec  byte
	Dummy    byte
	Ktbitflg uint16
	Ktbitun  uint16
	Ktbitbas uint32
}

type KTBBH struct {
	Ktbbhtyp uint8
	Dummy    [3]byte
	Ktbbhsid uint32
	Kscnbas  uint32
	Kscnwrp  uint16
	Dummy2   uint16
	Ktbbhict int16
	Ktbbhflg byte
	Ktbbhfsl byte
	Ktbbhfnx uint32
}

type KDBH struct {
	Kdbhflag uint8
	Kdbhntab int8
	Kdbhnrow int16
	Kdbhfrre int16
	Kdbhfseo int16
	Kdbhfsbo int16
	Kdbhavsp int16
	Kdbhtosp int16
}

type KDBT struct {
	Kdbtoffs int16
	Kdbtnrow int16
}

type BlockData struct {
	Kcbh      KCBH
	Ktbbh     KTBBH
	Ktbbhitl  []KTBBHITL
	Kdbh      KDBH
	Kdbt      []KDBT
	Kdbr      []int16
	rows      int16
	delRows   int16
	chainRows int16
	visualC   *color.Color
	visualS   string
	objName   string
	objOwner  string
	objId     uint32
}

var ObjNames map[uint32]string

func (b *BlockData) ParseBlock(file io.ReadSeeker, offset int64, block_size int64) {
	file.Seek(offset, io.SeekStart)
	binary.Read(file, binary.LittleEndian, &b.Kcbh)
	b.objId = 0
	b.rows = 0
	b.delRows = 0
	b.chainRows = 0
	if b.Kcbh.Type_kcbh == 6 {
		binary.Read(file, binary.LittleEndian, &b.Ktbbh)
		b.objId = b.Ktbbh.Ktbbhsid
		if b.Ktbbh.Ktbbhtyp == 1 && b.Ktbbh.Ktbbhict > 0 {
			b.Ktbbhitl = make([]KTBBHITL, b.Ktbbh.Ktbbhict)
			binary.Read(file, binary.LittleEndian, &b.Ktbbhitl)
			type offset_flags struct {
				Offset_flg1 uint32
				Offset_flg2 uint32
			}
			of := offset_flags{}
			var offset_mod int64
			binary.Read(file, binary.LittleEndian, &of)
			if of.Offset_flg1 == 0 && of.Offset_flg2 == 0 {
				offset_mod = 0
			} else if of.Offset_flg1 == 0 && of.Offset_flg2 > 0 {
				offset_mod = -4
			} else if of.Offset_flg1 > 0 && of.Offset_flg2 > 0 {
				offset_mod = -8
			}

			file.Seek(offset_mod, io.SeekCurrent)
			binary.Read(file, binary.LittleEndian, &b.Kdbh)
			b.Kdbt = make([]KDBT, b.Kdbh.Kdbhntab)
			binary.Read(file, binary.LittleEndian, &b.Kdbt)

			b.Kdbr = make([]int16, b.Kdbh.Kdbhnrow)
			binary.Read(file, binary.LittleEndian, &b.Kdbr)
			var row_pointer int64
			var row_header byte
			for i := int16(0); i < b.Kdbh.Kdbhnrow; i++ {
				row_pointer = int64(b.Kdbr[i]) + 100 + 24*int64(b.Ktbbh.Ktbbhict-2) + int64(offset_mod)
				file.Seek(offset+row_pointer, io.SeekStart)
				binary.Read(file, binary.LittleEndian, &row_header)
				if row_header == 44 {
					b.rows++
				} else if row_header == 60 {
					b.delRows++
				} else if row_header == 32 ||
					row_header == 8 ||
					row_header == 4 ||
					row_header == 2 ||
					row_header == 1 ||
					row_header == 12 {
					b.rows++
					b.chainRows++
				} else if row_header == 48 {
					b.delRows++
					b.chainRows++
				}
			}
		} else if b.Ktbbh.Ktbbhtyp == 2 {
			b.objId = b.Ktbbh.Ktbbhsid
		}
	} else if b.Kcbh.Type_kcbh == 32 { //1st level bitmap block
		file.Seek(offset+192, io.SeekStart)
		binary.Read(file, binary.LittleEndian, &b.objId)
	} else if b.Kcbh.Type_kcbh == 33 { //2nd level bitmap block
		file.Seek(offset+104, io.SeekStart)
		binary.Read(file, binary.LittleEndian, &b.objId)
	} else if b.Kcbh.Type_kcbh == 34 { //3d level bitmap block
		file.Seek(offset+192, io.SeekStart)
		binary.Read(file, binary.LittleEndian, &b.objId)
	} else if b.Kcbh.Type_kcbh == 35 { //pagetable segment header
		file.Seek(offset+272, io.SeekStart)
		binary.Read(file, binary.LittleEndian, &b.objId)
	}
	var objName string
	var foundName bool
	if b.objId != 0 {
		objName, foundName = ObjNames[b.objId]
	}

	if foundName {
		b.objOwner = strings.Split(objName, ".")[0]
		b.objName = strings.Split(objName, ".")[1]
		b.colorBlock()
	} else if !foundName && b.objId != 0 {
		q := "select nvl(max(owner), ' '), " +
			"nvl(max(object_name),'0GHOST0') " +
			"from dba_objects " +
			"where data_object_id=:1"
		rows, err := DB.Query(q, b.objId)
		if err == nil {
			rows.Next()
			rows.Scan(&b.objOwner, &b.objName)
		}
		ObjNames[b.objId] = b.objOwner + "." + b.objName

		b.colorBlock()
		//fmt.Println(b)
	} else {
		b.visualS = "o "
		b.visualC = color.New(color.Reset)
	}
}

var ColorID int8
var ColorMap map[string]int8

type Legend struct {
	visualC *color.Color
	visualS string
	objName string
	cnt     uint
}

var LegendMap map[string]Legend

func (b *BlockData) colorBlock() {
	colors := []*color.Color{color.New(color.FgRed),
		color.New(color.FgGreen),
		color.New(color.FgYellow),
		color.New(color.FgBlue),
		color.New(color.FgMagenta),
		color.New(color.FgCyan),
		color.New(color.FgWhite)}

	keyWord := b.objOwner + "." + b.objName + "(" + strconv.FormatUint(uint64(b.objId), 10) + ") "

	color_id, choosen := ColorMap[b.objName]
	if choosen {
		b.visualC = colors[color_id]
	} else {
		ColorMap[b.objName] = ColorID
		b.visualC = colors[ColorID]
		ColorID++
		if int(ColorID) == len(colors)-1 {
			ColorID = 0
		}
	}
	if b.Kcbh.Type_kcbh == 32 {
		b.visualS = "! "
		keyWord += " first level bitmap block "
	} else if b.Kcbh.Type_kcbh == 33 {
		b.visualS = "@ "
		keyWord += " second level bitmat block "
	} else if b.Kcbh.Type_kcbh == 34 {
		b.visualS = "# "
		keyWord += " third level bitmap block "
	} else if b.Kcbh.Type_kcbh == 35 {
		b.visualS = "$ "
		keyWord += " pagetable segment header "
	} else {
		b.visualS = string(b.objName[0]) + string(b.objName[len(b.objName)-1])
		keyWord += " regular block "
	}

	if b.Ktbbh.Ktbbhtyp == 2 {
		b.visualC = b.visualC.Add(color.Italic)
		b.visualS = strings.ToLower(b.visualS)
		keyWord += " (index block) "
	}

	if b.delRows > b.rows {
		b.visualC = b.visualC.Add(color.Underline)
		keyWord += " contains more deleted then actual rows "
	}

	if b.chainRows > 0 {
		b.visualC = b.visualC.Add(color.Bold).Add(color.Italic).Add(color.Faint)
		keyWord += " contains chained rows "
	}

	if b.objName == "0GHOST0" {
		b.visualC = b.visualC.Add(color.Bold).Add(color.BlinkSlow).Add(color.Underline)
		keyWord += " contains ghost data "
	}

	if b.delRows == 0 && b.rows == 0 && b.Kcbh.Type_kcbh == 6 && b.Ktbbh.Ktbbhtyp == 1 {
		b.visualC = b.visualC.Add(color.BgHiCyan)
		keyWord += " declared as empy - no rows here "
	}
	legend, exists := LegendMap[keyWord]
	if exists {
		legend.cnt++
		LegendMap[keyWord] = legend
	} else {
		LegendMap[keyWord] = Legend{
			b.visualC, b.visualS, b.objName, 1,
		}
	}

}

func main() {
	banner()
	ColorMap = make(map[string]int8)
	LegendMap = make(map[string]Legend)
	ObjNames = make(map[uint32]string)
	ColorID = 0
	lineSize := int64(32)
	wordSize := int64(8)
	var fname string
	var conn string
	var conn_asm string
	printBlocks := true
	conn_asm = "empty"
	var block_size int64
	if len(os.Args) < 6 {
		usage()
		return
	}
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "-f" {
			fname = os.Args[i+1]
		} else if os.Args[i] == "-c" {
			conn = os.Args[i+1]
		} else if os.Args[i] == "-b" {
			block_size, _ = strconv.ParseInt(os.Args[i+1], 10, 32)
		} else if os.Args[i] == "-a" {
			conn_asm = os.Args[i+1]
		} else if os.Args[i] == "-rs" {
			printBlocks = false
		}
	}

	connectDb(conn)
	defer DB.Close()

	var blocks int64
	if conn_asm != "empty" && fname[0] == '+' {
		connectAsm(conn_asm)
		defer ASM.Close()
	} else if conn_asm == "empty" && fname[0] == '+' {
		log.Panic("You have to specify ASM connect string in a form user/pass@ip:port/+ASM[n]")
	}

	if fname[0] != '+' {
		f, err := os.Open(fname)
		if err != nil {
			log.Panic(err)
		}
		defer f.Close()
		fs, _ := f.Stat()
		fsize := fs.Size()
		blocks = int64(fsize) / block_size

		block_data := BlockData{}
		for i := int64(0); i < blocks; i++ {
			blockBytes := make([]byte, block_size)
			f.Read(blockBytes)
			blockReader := bytes.NewReader(blockBytes)
			block_data.ParseBlock(blockReader, 0, block_size)
			if printBlocks {
				c := block_data.visualC.SprintFunc()
				if i%lineSize == 0 || i == 0 {
					fmt.Printf("%08d - %08d: %s", i+1, i+lineSize, c(block_data.visualS))
				} else if i > 0 && (i+1)%(lineSize) == 0 {
					fmt.Println("o ")
				} else if (i+1)%wordSize == 0 {
					fmt.Printf("%2s%s", c(block_data.visualS), " ")
				} else {
					fmt.Printf("%2s", c(block_data.visualS))
				}
			}
		}
	} else {
		fsize := -1
		ftype := -1
		fblock := -1
		fblock2 := -1
		fhandle := -1
		fsize2 := -1
		ASM.Exec("begin dbms_diskgroup.getfileattr(:1, :2, :3, :4); end;", fname,
			sql.Out{Dest: &ftype},
			sql.Out{Dest: &fsize},
			sql.Out{Dest: &fblock})
		blocks = int64(fsize)
		ASM.Exec("begin dbms_diskgroup.open(:1, 'r', :2, :3, :4, :5, :6); end;", fname, ftype, fblock,
			sql.Out{Dest: &fhandle},
			sql.Out{Dest: &fblock2},
			sql.Out{Dest: &fsize2})
		var block_data_s string
		block_data := BlockData{}
		for i := int64(0); i < blocks; i++ {
			_, err := ASM.Exec("begin dbms_diskgroup.read(:1, :2, :3, :4); end;", fhandle, i+1, block_size,
				sql.Out{Dest: &block_data_s})
			if err != nil {
				log.Panic(err)
			}

			blockBytes, _ := hex.DecodeString(block_data_s)
			blockReader := bytes.NewReader(blockBytes)
			block_data.ParseBlock(blockReader, 0, block_size)
			if printBlocks {
				c := block_data.visualC.SprintFunc()
				if i%lineSize == 0 || i == 0 {
					fmt.Printf("%08d - %08d: %s", i+1, i+lineSize, c(block_data.visualS))
				} else if i > 0 && (i+1)%(lineSize) == 0 {
					fmt.Println("o ")
				} else if (i+1)%wordSize == 0 {
					fmt.Printf("%2s%s", c(block_data.visualS), " ")
				} else {
					fmt.Printf("%2s", c(block_data.visualS))
				}
			}
		}
		ASM.Exec("begin dbms_diskgroup.close(:1) end;", fhandle)
	}

	fmt.Println("\n----- LEGEND -----")

	var legendKeys []string
	for k := range LegendMap {
		legendKeys = append(legendKeys, k)
	}
	sort.Strings(legendKeys)

	for _, k := range legendKeys {
		v := LegendMap[k]
		c := v.visualC.SprintFunc()
		fmt.Printf("%s - %25s blocks found: %d\n", c(v.visualS), k, v.cnt)
	}
	fmt.Println(".")

}
