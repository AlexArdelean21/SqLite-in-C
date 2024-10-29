# SQLite B-Tree Project

This project implements a simple B-Tree-based database inspired by SQLite. It includes operations for inserting, deleting, and selecting rows, as well as splitting and maintaining balanced B-Trees.

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Compilation and Running](#compilation-and-running)
- [Usage](#usage)
- [Commands](#commands)
- [Code Structure](#code-structure)        
- [Known Issues](#known-issues)
- [Contributing](#contributing)
- [License](#license)

## Features
- B-Tree-based database backend.
- Supports `insert`, `select`, and `delete` SQL-like commands.
- Implements internal and leaf node structures to create and manage a B-Tree.
- Utilizes Windows system calls for file I/O operations.

## Prerequisites
- **Windows OS**: This project uses Windows-specific libraries like `windows.h`.
- **Visual Studio**: The recommended development environment for this project is Visual Studio.
- **C Compiler**: A C compiler that supports the C99 standard.

## Compilation and Running
To compile the project, use the Visual Studio development environment. Follow these steps:

1. Clone the repository or download the source code files.
2. Open the project in Visual Studio.
3. Compile the project by selecting "Build" from the menu.
4. To run the project, provide a database file as an argument:

   ```sh
   ./SQLiteBTree myDatabase.db
   ```

   If no argument is provided, the program will default to using `myDatabase.db` as the database file.

## Usage
Upon running, you will be prompted to enter commands. The available commands include:

- **Insert a row**:
  ```
  insert <id> <username> <email>
  ```
  Example:
  ```
  insert 1 alice alice@example.com
  ```

- **Select all rows**:
  ```
  select
  ```

- **Delete a row**:
  ```
  delete <id>
  ```
  Example:
  ```
  delete 1
  ```

- **Exit the program**:
  ```
  .exit
  ```

- **Print the B-Tree structure**:
  ```
  .btree
  ```

- **Print constants used in the project**:
  ```
  .constants
  ```

## Commands
- `.exit`: Exit the program and save changes.
- `.btree`: Display the B-Tree structure for debugging purposes.
- `.constants`: Display constants used in the project.

## Code Structure
- **BTree**: Contains functions for managing the B-Tree, including insertions, deletions, and searching nodes.
- **Pager**: Manages reading and writing pages to the disk.
- **Cursor**: Handles row navigation within the B-Tree.
- **DB Commands**: Executes SQL-like commands such as `insert`, `select`, and `delete`.
- **Input Buffer**: Handles user input, buffers commands, and processes them.

## Known Issues
- Only supports a small fixed-size database due to the limitation on pages.
- The code is Windows-specific and will need modification to work on other operating systems.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
It was made with the help of a tutorial found here https://cstack.github.io/db_tutorial/
