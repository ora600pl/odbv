#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <sstream>
#include <cmath>
#include <map>
using namespace std;

string bgColor="BLACK";

void banner()
{
	cout << endl << endl << "THIS TOOL IS NOT FOR PRODUCTION USE!!! Only for fun and learn ;)" << endl;
        cout << "created by Kamil Stawiarski (@ora600pl kstawiarski@ora-600.pl ora-600.pl)" << endl;
}

void help()
{
        cout << "-f     Path to a datafile" << endl;
        cout << "-b     Block size" << endl;
        cout << "-bg    What background color do you use? BLACK (default) or WHITE?" << endl ;
        cout << "-d     Detail mode - displays block numbers" << endl;
        cout << "-ls     Set line size (default 128)" << endl;
        cout << "-o     Path to file with DATA_OBJECT_IDs to visualize in form: " << endl 
	     << "DATA_OBJECT_ID:letter_for_object_symbol|COLOR (example - 93948:X|RED)" << endl 
	     << "colors available: BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE (data blocks with no rows will have white background)" << endl;

	banner();
}

string getColor(string name, bool empty, bool manyDelRows)
{
	map<string,string>colors;
	colors["BLACK"]="\033[30";
	colors["RED"]="\033[31";
	colors["GREEN"]="\033[32";
	colors["YELLOW"]="\033[33";
	colors["BLUE"]="\033[34";
	colors["MAGENTA"]="\033[35";
	colors["CYAN"]="\033[36";
	colors["WHITE"]="\033[37";

	if(colors.count(name)>0 && !empty && !manyDelRows) 
	{
		return colors[name] + "m";
	}
	else if(colors.count(name)>0 && empty)
	{
	    if(bgColor=="BLACK")
		return colors[name] + ";5;47m";
	   else
		return colors[name] + ";5;40m";
	}
	else if(colors.count(name)>0 && !empty && manyDelRows)
        {
            if(bgColor=="BLACK")
                return colors[name] + ";7;47m";
           else
                return colors[name] + ";7;40m";
        }
	else
	{
		return "\033[0m";
	}
}

void readData(string p_objIdFile, string p_fname, int p_blockSize,bool debug, int lineS)
{
        int objIdOffset=24; //offset for data_object_id
        int blockSize=8192; //block size
        int numOfRowsOffset=54; //you have to add itl count at offset 36
        int rowDataOffset=70; //you have to add itl count at offset 36
        int ktbbhictOffset=36; //offset of numer of ITL slots
        int kdbhntabOffset=53; //offset of the kdbt - 4 byte structure which has to be added to rowDataOffset to find data
	int ktbbhtypOffset=20; //for block type 6, byte 20 specifies 1 for table data and 2 for index data

	string fileNameDbf=p_fname;
	string fileNameIds=p_objIdFile;

	ifstream ifObjIds(fileNameIds.c_str(), ios::in); //file containing object ids with symbols

	map<int,char> objIds; //map of object ids and it's symbol
	map<int,string> objColors; //map of colors for object symbols
	map<int,int> objBlocks; //map for number of blocks
	map<int,int> objEmptyBlock; //map for number of blocks marked as empty
	map<int,int> objLowRBlock; //map for number of blocks with declared rows to actual 2C rows less then 0.5
	string objIdsLine; //whole line from a file

	int lineSize=lineS;
	int wordSize=8;
	if(debug)
	{
		wordSize=2;
	}
	

	if(ifObjIds.is_open())
	{
		while(getline(ifObjIds,objIdsLine))
		{
			objIds[stoi(objIdsLine.substr(0,objIdsLine.find(":")))]=objIdsLine.substr(objIdsLine.find(":")+1,1)[0];
			objBlocks[stoi(objIdsLine.substr(0,objIdsLine.find(":")))]=0;
			objEmptyBlock[stoi(objIdsLine.substr(0,objIdsLine.find(":")))]=0;
			objLowRBlock[stoi(objIdsLine.substr(0,objIdsLine.find(":")))]=0;
			objColors[stoi(objIdsLine.substr(0,objIdsLine.find(":")))]=objIdsLine.substr(objIdsLine.find("|")+1);
		}
	}
	ifObjIds.close();
	ifstream datafile(fileNameDbf.c_str(), ios::binary);
	if(datafile.is_open())
	{
		int i = 0;
		while(!datafile.eof() && datafile.good())
		{


			if(i==0)
                        {
                                printf("%08d - %08d:  ",i+1,i+lineSize);
                        }

			datafile.seekg(i*blockSize); //set pointer at the block begining
                        char blockType[1]; 
                        int  blockTypeI;
                        datafile.read(blockType,1); //first byte is block type - 6 is for table data or index data
                        blockTypeI = (int) blockType[0];

			char indexOrTable[1]; //byte 20 for type 6 specifies 1 for table data and 2 for index data
			int  indexOrTableI;

			datafile.seekg(i*blockSize+ktbbhtypOffset);
			datafile.read(indexOrTable,1);
			indexOrTableI = (int) indexOrTable[0];

			char numOfRows[1];
                        unsigned int  numOfRowsI;
			int actualRows2c=0; //number of actual rows with 2C flag (2C - row is not deleted)
			if(blockTypeI==6 && indexOrTableI==2)
			{	
				cout << "+";
			}
			else if(blockTypeI==32) // FIRST LEVEL BITMAP BLOCK
			{
				datafile.seekg(i*blockSize+192); //192 is offset of the object id in block type 32
				char objId[4];
                                int  objIdI;
                                datafile.read(objId,4);
                                memcpy(&objIdI,objId,sizeof(objId));
				if(objIds.count(objIdI)>0)
				{
					cout << getColor(objColors[objIdI],false,true) << "!" << "\033[0m";
				}
				else
				{
					cout << "!";
				}

			}
			else if(blockTypeI==33) // SECOND LEVEL BITMAP BLOCK
                        {
                                datafile.seekg(i*blockSize+104); //104 is offset of the object id in block type 33
                                char objId[4];
                                int  objIdI;
                                datafile.read(objId,4);
                                memcpy(&objIdI,objId,sizeof(objId));
                                if(objIds.count(objIdI)>0)
                                {
                                        cout << getColor(objColors[objIdI],false,true) << "@" << "\033[0m";
                                }
				else 
				{
					cout << "@";
				}

                        }
			else if(blockTypeI==34) // THIRD LEVEL BITMAP BLOCK
                        {
                                datafile.seekg(i*blockSize+192);
                                char objId[4];
                                int  objIdI;
                                datafile.read(objId,4);
                                memcpy(&objIdI,objId,sizeof(objId));
                                if(objIds.count(objIdI)>0)
                                {
                                        cout << getColor(objColors[objIdI],false,true) << "#" << "\033[0m";
                                }
				else
				{
					cout << "#";
				}

                        }
			else if(blockTypeI==35) // PAGETABLE SEGMENT HEADER
                        {
                                datafile.seekg(i*blockSize+272); //272 is offset of the object id in block type 35
                                char objId[4];
                                int  objIdI;
                                datafile.read(objId,4);
                                memcpy(&objIdI,objId,sizeof(objId));
                                if(objIds.count(objIdI)>0)
                                {
                                        cout << getColor(objColors[objIdI],false,true) << "$" << "\033[0m";
                                }
				else
				{
					cout << "$";
				}
                        }
			else if(blockTypeI==6 && indexOrTableI==1) 
			{
				datafile.seekg(i*blockSize+objIdOffset); //set pointer at data_object_id offset (24)
				char objId[4];
				int  objIdI;
				datafile.read(objId,4);
				memcpy(&objIdI,objId,sizeof(objId));

				char ktbbhictCount[1]; 
                                int  ktbbhictCountI;

                                datafile.seekg(i*blockSize+ktbbhictOffset); //set offset at ktbbhict - necessary to calculate row pointer position in a block
                                datafile.read(ktbbhictCount,1);
                                ktbbhictCountI=(int) ktbbhictCount[0];

                                datafile.seekg(i*blockSize+numOfRowsOffset+24*ktbbhictCountI); //24 is the size of ktbbhitl struct
                                datafile.read(numOfRows,1); //read the declared number of rows
                                numOfRowsI=(unsigned int) (unsigned char) numOfRows[0];


				if(numOfRowsI>0 && objIds.count(objIdI)>0)
				{
					char numOfkdbt[1]; //number of tables - necessary to calculate position of row pointer
                                        int  numOfkdbtI;
                                        datafile.seekg(i*blockSize+kdbhntabOffset+24*ktbbhictCountI);
                                        datafile.read(numOfkdbt,1);
                                        numOfkdbtI = (int) numOfkdbt[0];
					int rowPointerPos=i*blockSize+rowDataOffset+24*ktbbhictCountI+4*(numOfkdbtI-1); //calculate position of row pointer

					char rowPointer[2]; //row pointer has 2 bytes
                                        int  rowPointerI;

					for(int row=0 ; row<numOfRowsI ; row++)
					{
						datafile.seekg(rowPointerPos); //set the first position
                                                datafile.read(rowPointer,2); //read the row pointer
						if((int)rowPointer[0] != 0 && (int)rowPointer[0]!=-1) //check if row pointer is valid
                                                {
                                                 	memcpy(&rowPointerI,rowPointer,sizeof(rowPointer)); 
							rowPointerI+=100+24*(ktbbhictCountI-2);
							datafile.seekg(rowPointerI+i*blockSize);
                                                        char rowPresent[1];
                                                        int  rowPresentI;
                                                        datafile.read(rowPresent,1);
                                                        rowPresentI=(int) rowPresent[0];
							if (rowPresentI==44 || rowPresentI==108)
							{
								actualRows2c++;
							}
						}
						rowPointerPos+=2;
					}			
				}

				if(objIds.count(objIdI)>0 && numOfRowsI>0)
				{
				    if((float)actualRows2c/numOfRowsI>=0.5)
				    {
					cout << getColor(objColors[objIdI],false,false) << objIds[objIdI];
					objBlocks[objIdI]++;
			  	    }
				    else
				    {
					cout << getColor(objColors[objIdI],false,true) << objIds[objIdI];
					objLowRBlock[objIdI]++;
				    }
				}
				else if(objIds.count(objIdI)>0 && numOfRowsI<=0)
				{
					cout << getColor(objColors[objIdI],true,false) << (char) tolower(objIds[objIdI]); 
					objEmptyBlock[objIdI]++;
				}
				else
				{
					cout << "O";
				}
				if(debug && objIds.count(objIdI)>0)
                        	{
                                	cout << "[" << i << "](" << numOfRowsI << "," << actualRows2c << ")" << endl;
                        	}

			}
			else if(i>0)
			{
				cout << "o";
			}
			cout << "\033[0m";
			if(i>0 && i % lineSize == 0)
			{
				cout << endl;
				printf("%08d - %08d:  ",i+1,i+lineSize);
			}
			else if (i>0 && i % wordSize == 0)
			{
				cout << " ";
			}

			i++;
			actualRows2c=0;
		}
	}

	datafile.close();

	cout << endl << endl << "Legend: " << endl;
	cout << "O: this is not the block you are looking for" << endl;
	cout << "o: this is empty block" << endl;
	cout << "+: this is index block" << endl;
	cout << "!: this is first level bitmap block" << endl;
	cout << "@: this is second level bitmap block" << endl;
	cout << "#: this is third level bitmap block" << endl;
	cout << "$: this is pagetable segment header" << endl << endl;
	
	for(map<int,char>::iterator it=objIds.begin(); it!=objIds.end(); ++it)
	{
		cout << it->first << " " << getColor(objColors[it->first],false,false) << it->second << "\033[0m" 
                     << "	regular block (this object has " <<  objBlocks[it->first] << " blocks)" <<  endl;
		cout << it->first << " " << getColor(objColors[it->first],false,true) << it->second << "\033[0m"
                     << "	block with less then 50% of rows present - most of them are deleted (3C): " << objLowRBlock[it->first] << " such blocks for this object "<<  endl;
		cout << it->first << " " << getColor(objColors[it->first],true,false) << (char) tolower(it->second) << "\033[0m"
                     << "	block with declared number of rows 0: " << objEmptyBlock[it->first] << " such blocks for this object "<<  endl << endl;
	}	

	banner();	

}

int main (int argc, char * argv[])
{
        string dumpingDict="-d"; //when dumping the dictionary, offests are different
        string dataFile="-f"; //switch for datafile path
        string block="-b"; //switch for block size
        string objectId="-o"; //switch for data_object_id
	string debugMode="-d"; //prints block numbers
	string bgColor="-bg"; //what backgroupnd color is used
	string lineSize="-ls"; //size of the line to display

	int lineS=128;

        int blockSize; //block size
        string objIdsFile; //File containing object ids
        string fileName;
	bool debug=false;

        if(argc>=6)
        {
                for(int i=1 ; i<argc ; i++)
                {
                        if(argv[i]==dataFile)
                        {
                                fileName=argv[i+1];
                        }
                        else if(argv[i]==block)
                        {
                                blockSize= stoi(argv[i+1]);
                        }
                        else if(argv[i]==objectId)
                        {
                                objIdsFile=argv[i+1];
                        }
			else if(argv[i]==debugMode)
                        {
                                debug=true;
                        }
			else if(argv[i]==debugMode)
                        {
                                bgColor=argv[i+1];
                        }
			else if(argv[i]==lineSize)
                        {
                                lineS=stoi(argv[i+1]);
                        }
                }
        }
        else
        {
                help();
                return 1;
        }
        readData(objIdsFile, fileName, blockSize,debug,lineS);
        return 0;

}
