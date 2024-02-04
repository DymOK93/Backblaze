# Backblaze
Cross-platform utility for parsing [HDD and SSD failure statistics from Backblaze](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data)

## Supported systems
Tested on Linux x64 (Debian 12) and Windows 10 x64

## Third-party
[rapid-csv](https://github.com/d99kris/rapidcsv/) and [unordered_dense](https://github.com/martinus/unordered_dense) are used as Git submodules

## Build
CMake 3.19 and newer and GCC, Clang or MSVC with C++20 support are required

## Usage
`Backblaze[.exe] <input_path> <output_path>`
* `input_path` - path to input file (should have .csv extension) or directory (will be recursively scanned for .csv files)
* `output_path` - path to output file (should have .csv extension)

Note: recursive mode uses all CPU cores to speed up processing.