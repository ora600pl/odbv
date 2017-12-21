import random
from struct import Struct
import os
import sys


class ODBV(object):
    def __init__(self):
        self.html_header = "<!DOCTYPE html><html><head><style>.tooltip {position: relative;" \
                           "display: inline-block;}.tooltip .tooltiptext {" \
                           "visibility: hidden; background-color: black; color: #fff; " \
                           "text-align: left; border-radius: 6px; " \
                           "padding: 5px; /* Position the tooltip */ right: 10%;" \
                           "position: absolute; z-index: 1;}.tooltip:hover .tooltiptext {visibility: visible;}" \
                           ".tabstyle {text-align: center; font-size: 11px; color: white;}" \
                           "</style></head><body>\n"

        self.object_names = {}
        self.object_colors = {}
        self.file_names = []
        self.ghost_objects = {}

        self.columns = 63

        self.uint = Struct("I")
        self.ubyte = Struct("B")
        self.ushort = Struct("H")
        self.block_size = 8192

        self.block_type = {6: "DATA", 32: "FIRST LEVEL BITMAP BLOCK", 33: "SECOND LEVEL BITMAP BLOCK",
                           24: "THIRD LEVEL BITMAP BLOCK", 35: "PAGETABLE SEGMENT HEADER"}

        # Internal block offsets:
        self.offset_objd = {6: 24, 32: 192, 33: 104, 34: 192, 35: 272}
        self.numOfRowsOffset = 54  # you have to add itl count at offset 36
        self.rowDataOffset = 70  # you have to add itl count at offset 36
        self.ktbbhictOffset = 36  # offset of numer of ITL slots
        self.kdbhntabOffset = 53  # offset of the kdbt - 4B structure which has to be added to rowDataOffset to find row
        self.ktbbhtypOffset = 20  # for block type 6, byte 20 specifies 1 for table data and 2 for index data

    @staticmethod
    def get_color():
        return '#{:02x}{:02x}{:02x}'.format(*map(lambda x: random.randint(0, 255), range(3)))

    def connect_to_oracle(self, connect_string):
        try:
            __import__('imp').find_module('cx_Oracle')
            import cx_Oracle
            con = cx_Oracle.connect(connect_string)
            cur = con.cursor()
            cur.execute("select data_object_id, lower(object_type) || ': ' || owner || '.' || object_name as oname "
                        "from dba_objects")
            for row in cur:
                self.object_names[row[0]] = row[1]
                self.object_colors[row[0]] = self.get_color()

            cur.close()

            con.close()

        except ImportError:
            print("You have to install cx_Oracle to handle Oracle database. Try #pip install cx_Oracle. "
                  "And setup ENV properly!")
        except BaseException as e:
            print("Something went wrong: " + str(e))

    def add_file(self, dbf):
        self.file_names.append(dbf)

    @staticmethod
    def help():
        print("ODBV v2 by Kamil Stawiarski (@ora600pl | www.ora-600.pl)")
        print("ODBV v2 requires cx_Oralce - you pip to install it")
        print("\n\nEnter interactive mode: $ python3 odbv2.py -i")
        print("Or do a batch processing: $ python3 -f file_with_path_to_dbfs -c user/pass@ip:port/service -o file.html")
        print("\n\nUsage for interactive mode: ")
        print("\tto add a datafile for parsing and visualizing:")
        print("\t\tODBV> add file <<path to a datafile>>")
        print("\tto generate html visualization report:")
        print("\t\tODBV> make html file_name.html")
        print("\tto connect to Oracle for dictionary data:")
        print("\t\tget dict user/password@ip:port/service")

        print("\n\tDefault block size is 8192 - to change it to N:")
        print("\t\tODBV> set blocksize N")

    def set_blocksize(self, bs):
        self.block_size = bs

    def get_row_details(self, block):
        num_of_itls = self.ubyte.unpack(block[self.ktbbhictOffset:self.ktbbhictOffset + 1])[0]

        delcared_rows_offset = 24 * num_of_itls + self.numOfRowsOffset
        declared_rows = self.ubyte.unpack(block[delcared_rows_offset:delcared_rows_offset + 1])[0]
        actual_rows = 0

        if declared_rows > 0:
            num_of_tables_offset = 24 * num_of_itls + self.kdbhntabOffset
            num_of_tables = self.ubyte.unpack(block[num_of_tables_offset:num_of_tables_offset + 1])[0]

            # first row pointer in a block
            row_pointer_offset = self.rowDataOffset + 24 * num_of_itls + 4 * (num_of_tables - 1)

            for row in range(declared_rows):
                try:
                    row_pointer = self.ushort.unpack(block[row_pointer_offset:row_pointer_offset + 2])[0]
                    row_pointer += 100 + 24 * (num_of_itls - 2)

                    row_flag = self.ubyte.unpack(block[row_pointer:row_pointer + 1])[0]
                    if row_flag == 44 or row_flag == 108:
                        actual_rows += 1

                    row_pointer_offset += 2

                except:
                    row_pointer_offset += 2

        return declared_rows, actual_rows

    def make_html(self, file_name):
        html_txt = self.html_header
        for dbf in self.file_names:
            f = open(dbf, "rb")
            f.seek(0, os.SEEK_END)
            fs = f.tell()
            html_txt += "file: " + dbf + " size: " + str(fs) + " bytes"
            file_pos = 0
            block_no = 1
            col_counter = 0
            html_txt += "<table border=1 class=\"tabstyle\">\n"
            while file_pos <= fs:
                file_pos = block_no * self.block_size
                f.seek(file_pos)
                block = f.read(self.block_size)
                declared_rows = -1
                actual_rows = -1

                try:
                    block_type_n = self.ubyte.unpack(block[0:1])[0]
                    block_type = self.block_type.get(block_type_n, "OTHER")

                except:
                    block_type = "OTHER"

                if block_type != "OTHER":

                    try:
                        objd_pos = self.offset_objd[block_type_n]
                        objd = self.uint.unpack(block[objd_pos:objd_pos + 4])[0]
                        obj_name = self.object_names.get(objd, "ghost:" + str(objd))
                        obj_color = self.object_colors.get(objd, "black")

                        index_or_data = self.ubyte.unpack(block[self.ktbbhtypOffset:self.ktbbhtypOffset + 1])[0]

                        if block_type == "DATA" and index_or_data == 1:
                            declared_rows, actual_rows = self.get_row_details(block)
                        elif block_type == "DATA" and index_or_data == 2:
                            block_type = "INDEX"

                    except:
                        obj_name = "empty"
                        obj_color = "white"

                else:
                    obj_name = "empty"
                    obj_color = "white"

                block_symbol = "X"

                if block_type == "DATA":
                    if declared_rows > 0:
                        block_symbol = str(round(actual_rows / declared_rows, 2))
                    else:
                        block_symbol = 0
                elif block_type == "FIRST LEVEL BITMAP BLOCK":
                    block_symbol = "!"
                elif block_type == "SECOND LEVEL BITMAP BLOCK":
                    block_symbol = "@"
                elif block_type == "THIRD LEVEL BITMAP BLOCK":
                    block_symbol = "#"
                elif block_type == "PAGETABLE SEGMENT HEADER":
                    block_symbol = "$"
                elif block_type == "INDEX":
                    block_symbol = "+"

                if col_counter == 0:
                    html_txt += "<tr><td height=\"30\"><font color=black>{:08d}-{:08d}</font></td>".format(block_no,
                                                                                                           block_no +
                                                                                                           self.columns)
                    html_txt += "<td bgcolor=\"{0}\"><div class=\"tooltip\"> {1} " \
                                "<span class=\"tooltiptext\"> block type: {2} <br /> block no: {3} <br /> {4} " \
                                "declared_rows:{5} <br /> actual_rows:{6}</span>" \
                                "</div></td>".format(obj_color, block_symbol, block_type, block_no, obj_name,
                                                     declared_rows, actual_rows)
                    col_counter += 1

                elif col_counter % self.columns == 0:
                    html_txt += "<td bgcolor=\"{0}\"><div class=\"tooltip\"> {1} " \
                                "<span class=\"tooltiptext\"> block type: {2} <br /> block no: {3} <br /> {4} " \
                                "declared_rows:{5} <br /> actual_rows:{6}</span>" \
                                "</div></td>".format(obj_color, block_symbol, block_type, block_no, obj_name,
                                                     declared_rows, actual_rows)

                    html_txt += "</tr>\n"
                    col_counter = 0

                else:
                    html_txt += "<td bgcolor=\"{0}\"><div class=\"tooltip\"> {1} " \
                                "<span class=\"tooltiptext\"> block type: {2} <br /> block no: {3} <br /> {4} " \
                                "declared_rows:{5} <br /> actual_rows:{6}</span>" \
                                "</div></td>".format(obj_color, block_symbol, block_type, block_no, obj_name,
                                                     declared_rows, actual_rows)

                    col_counter += 1

                block_no += 1

            html_txt += "</table>"

        html_txt += "</body></html>"

        html_file = open(file_name, "w")
        html_file.write(html_txt)
        html_file.close()


if __name__ == '__main__':
    odbv = ODBV()
    cnt = False
    if len(sys.argv) > 1 and sys.argv[1] == "-i":
        cnt = True
    elif len(sys.argv) == 7:
        try:
            out_file = "out.html"
            for i in range(1, 6):
                if sys.argv[i] == "-f":
                    dbfs = open(sys.argv[i + 1], "r").readlines()
                    for f in dbfs:
                        odbv.add_file(f[:-1])
                elif sys.argv[i] == "-c":
                    odbv.connect_to_oracle(sys.argv[i + 1])
                elif sys.argv[i] == "-o":
                    out_file = sys.argv[i + 1]

            odbv.make_html(out_file)
        except BaseException as e:
            print("You messed up... Or I messed up. Something is messed up")
            print(str(e))
            odbv.help()
            raise

    else:
        odbv.help()

    while cnt:
        try:
            command = input("ODBV > ").strip()
            if command == "exit":
                cnt = False
            elif command == "help":
                odbv.help()
            elif command.startswith("add file"):
                odbv.add_file(command.split()[2])
            elif command.startswith("get dict"):
                odbv.connect_to_oracle(command.split()[2])
            elif command.startswith("make html"):
                odbv.make_html(command.split()[2])
            elif command.startswith("set blocksize"):
                odbv.set_blocksize(int(command.split()[2]))
            elif len(command) > 0:
                print("\n\nWhat???")
                odbv.help()

        except BaseException as e:
            print("You messed up... Or I messed up. Something is messed up")
            print(str(e))
            odbv.help()
