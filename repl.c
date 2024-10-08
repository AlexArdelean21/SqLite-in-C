#define _CRT_SECURE_NO_WARNINGS
#include <errno.h>
#include <windows.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>


#pragma region Defined variables

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
}Row;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

#define ID_SIZE size_of_attribute(Row, id)
#define USERNAME_SIZE size_of_attribute(Row, username)
#define EMAIL_SIZE size_of_attribute(Row, email)
#define ID_OFFSET 0
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
#define ROW_SIZE (ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)
#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100
#define ROWS_PER_PAGE (PAGE_SIZE / ROW_SIZE)
#define TABLE_MAX_ROWS (ROWS_PER_PAGE * TABLE_MAX_PAGES)


#pragma endregion

#pragma region enums and structs

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL }ExecuteResult;
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;
typedef enum { META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED_COMMAND } MetaCommandResult;
typedef enum { PREPARE_SUCCESS, PREPARE_NEGATIVE_ID, PREPARE_STRING_TOO_LONG, PREPARE_SYNTAX_ERROR, PREPARE_UNRECOGNIZED_STATEMENT } PrepareResult;


typedef struct {
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef struct {
    HANDLE  file_handle;
    uint32_t file_lenght;
    void* pages[TABLE_MAX_PAGES];
}Pager;

typedef struct {
    StatementType type;
    Row row_to_insert;
}Statement;

typedef struct {
    Pager* pager;
    uint32_t num_rows;
}Table;

typedef struct {
    Table* table;
    uint32_t row_num;
    bool end_of_table;
} Cursor;

#pragma endregion

#pragma region buffer

InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer* input_buffer) {
    if (input_buffer->buffer == NULL) {
        input_buffer->buffer = malloc(1024);  // Allocate 1024 bytes initially
        if (input_buffer->buffer == NULL) {
            printf("Error allocating memory\n");
            exit(EXIT_FAILURE);
        }
        input_buffer->buffer_length = 1024;
    }

    if (fgets(input_buffer->buffer, input_buffer->buffer_length, stdin) == NULL) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    input_buffer->input_length = strlen(input_buffer->buffer);

    if (input_buffer->buffer[input_buffer->input_length - 1] == '\n') {
        input_buffer->buffer[input_buffer->input_length - 1] = 0;
        input_buffer->input_length -= 1;
    }
}

void close_input_buffer(InputBuffer* new_input_buffer) {
    free(new_input_buffer->buffer);
    free(new_input_buffer);
}

#pragma endregion

#pragma region Pager

Pager* pager_open(const char* filename) {
    HANDLE file_handle = CreateFile(
        filename,
        GENERIC_READ | GENERIC_WRITE,
        0,
        NULL,
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        NULL
    );

    if (file_handle == INVALID_HANDLE_VALUE) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    DWORD file_size = GetFileSize(file_handle, NULL);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_handle = file_handle;
    pager->file_lenght = file_size;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    SetFilePointer(pager->file_handle, page_num * PAGE_SIZE, NULL, FILE_BEGIN);

    DWORD bytes_written;
    if (!WriteFile(pager->file_handle, pager->pages[page_num], size, &bytes_written, NULL)) {
        printf("Error writing to file\n");
        exit(EXIT_FAILURE);
    }
}

void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row* source, void* destination) { // void* is useful for flexibility and generic programming, 
    memcpy((char*)destination + ID_OFFSET, &(source->id), ID_SIZE); // allowing the functions to handle multiple data types. 
    memcpy((char*)destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy((char*)destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->email), (char*)source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        void* page = malloc(PAGE_SIZE);
        DWORD num_pages = pager->file_lenght / PAGE_SIZE;

        if (pager->file_lenght % PAGE_SIZE) {// if this isn't true we have a partial page
            num_pages++;
        }

        if (page_num <= num_pages) {
            SetFilePointer(pager->file_handle, page_num * PAGE_SIZE, NULL, FILE_BEGIN);
            DWORD bytes_read;
            if (!ReadFile(pager->file_handle, page, PAGE_SIZE, &bytes_read, NULL)) {
                printf("Error reading file\n");
                exit(FatalExit);
            }
        }
        pager->pages[page_num] = page;

    }
    return pager->pages[page_num];

}
#pragma endregion

#pragma region DB
Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    uint32_t num_rows = pager->file_lenght / ROW_SIZE;

    Table* table = malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;

    return table;
}

void db_close(Table* table) {
    Pager* pager = table->pager;
    uint32_t num_full_gages = table->num_rows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_full_gages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }

        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
    CloseHandle(pager->file_handle);

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}
#pragma endregion

#pragma region Commands
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.email, email);
    strcpy(statement->row_to_insert.username, username);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement); 
    }

    if (strncmp(input_buffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

#pragma endregion

#pragma region Cursor

Cursor* table_start(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = (table->num_rows == 0);

    return cursor;
}

Cursor* table_end(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;

    return cursor;
}

void* cursor_value(Cursor* cursor) {
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = get_page(cursor->table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return (char*)page + byte_offset;
}

void cursor_advance(Cursor* cursor) {
    cursor->row_num += 1;
    if (cursor->row_num >= cursor->table->num_rows) {
        cursor->end_of_table = true;
    }
}


#pragma endregion

#pragma region Statements
ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);
    Cursor* cursor = table_end(table);
    serialize_row(row_to_insert,cursor_value(cursor));
    table->num_rows++;
    free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Cursor* cursor = table_start(table);
    Row row;

    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
    switch (statement->type)
    {
    case(STATEMENT_INSERT):
        return execute_insert(statement, table);
    case(STATEMENT_SELECT):
        return execute_select(statement, table);
    }
}
#pragma endregion

int main(int argc, char* args[]) {
    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = args[1];
    Table* table = db_open(filename);

    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.') {

            switch (do_meta_command(input_buffer, table))
            {
            case (META_COMMAND_SUCCESS):
                continue;
            case (META_COMMAND_UNRECOGNIZED_COMMAND):
                printf("Unrecongnized command '%s' \n", input_buffer->buffer);
                continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
        case (PREPARE_SUCCESS):
            break;
        case (PREPARE_NEGATIVE_ID):
            printf("ID must be positeve\n");
            continue;
        case (PREPARE_STRING_TOO_LONG):
            printf("String is too long\n");
            continue;
        case (PREPARE_SYNTAX_ERROR):
            printf("Syntax error. Could not parse statement.\n");
            continue;
        case (PREPARE_UNRECOGNIZED_STATEMENT):
            printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
            continue;
        }

        switch (execute_statement(&statement, table))
        {
        case (EXECUTE_SUCCESS):
            printf("Executed.\n");
            break;
        case (EXECUTE_TABLE_FULL):
            printf("error: Table full.\n");
            break;
        }
        
    }   
}