#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <string>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 1000
#define GLOBAL_DEPT 1
#define FILES_NUM 10

using namespace std;

const char* names[] = 
{
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = 
{
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = 
{
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
{                           \
  HT_ErrorCode code = call; \
  if (code != HT_OK) {      \
    printf("Error\n");      \
    exit(code);             \
  }                         \
}

int main() 
{
  BF_Init(LRU);
  
  CALL_OR_DIE(HT_Init());

  int indexDesc[FILES_NUM];
  for (int i = 0; i < FILES_NUM; i++) // Loop for multiple files
  {
    cout << endl << "File: " << i + 1 << endl << endl;
    string defaultFileName("data");
    string fileName(defaultFileName + "_" + std::to_string(i + 1) + ".db");
    CALL_OR_DIE(HT_CreateIndex(fileName.c_str(), GLOBAL_DEPT));
    CALL_OR_DIE(HT_OpenIndex(fileName.c_str(), &indexDesc[i])); 

    Record record;
    srand(12569874 + i);
    int r;
    printf("Insert Entries\n");
    for (int id = 0; id < RECORDS_NUM; ++id) 
    {
      // create a record
      record.id = id;
      r = rand() % 12;
      memcpy(record.name, names[r], strlen(names[r]) + 1);
      r = rand() % 12;
      memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
      r = rand() % 10;
      memcpy(record.city, cities[r], strlen(cities[r]) + 1);

      CALL_OR_DIE(HT_InsertEntry(indexDesc[i], record));
    }

    printf("RUN PrintAllEntries\n");
    int id = rand() % RECORDS_NUM;
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc[i], &id));
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc[i], NULL));
  }

  for (int i = 0; i < FILES_NUM; i++) // Close all
  {
    CALL_OR_DIE(HT_CloseFile(indexDesc[i]));
  }

  for (int i = 0; i < FILES_NUM; i++) // Reopen, print records and statistics
  {
    cout << endl << "File: " << i + 1 << endl << endl;
    string defaultFileName("data");
    string fileName(defaultFileName + "_" + std::to_string(i + 1) + ".db");

    CALL_OR_DIE(HT_OpenIndex(fileName.c_str(), &indexDesc[i])); 
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc[i], NULL));
    CALL_OR_DIE(HT_CloseFile(indexDesc[i]));
    CALL_OR_DIE(HashStatistics(fileName.c_str()));
  }    
  
  BF_Close();
}
