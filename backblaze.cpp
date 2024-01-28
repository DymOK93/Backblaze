#include "backblaze.hpp"

#include "csv-parser/include/csv.hpp"
#include "unordered_dense/include/ankerl/unordered_dense.h"

#include <array>
#include <chrono>
#include <fstream>
#include <stdexcept>

using namespace std;

int main(int argc, char* argv[]) {
  try {
    if (argc != 3) {
      throw invalid_argument{"Usage: <input-path> <output-path>"};
    }

    const filesystem::path input{argv[1]};
    const filesystem::path output{argv[2]};
    fmt::print("Input: {}\nOutput: {}\n", input.string(), output.string());

    const util::Timer<chrono::seconds> timer;
    const bb::ModelStats stats{
        parseStats(filesystem::recursive_directory_iterator{input})};
    fmt::print("Finished: {} seconds\n", timer.elapsed().count());
    writeParsedStats(stats, output, true);

  } catch (const std::exception& exc) {
    printf("%s\n", exc.what());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

namespace bb {
template <class Parser>
static void readStatsImpl(ModelStats& map,
                          const filesystem::path& file_path,
                          Parser parser) {
  std::ifstream input(file_path, std::ios::in | std::ios::binary);
  csv::CSVReader reader{input, csv::CSVFormat{}.header_row(0).delimiter(',')};

  for (const csv::CSVRow& row : reader) {  // Input iterator
    auto model = row["model"].get<string>();
    const Stats stats{parser(row)};
    map[std::move(model)].merge(stats);
  }
}

void readRawStats(ModelStats& map, const filesystem::path& file_path) {
  readStatsImpl(map, file_path, [](const csv::CSVRow& row) -> Stats {
    const auto failure = row["failure"].get<uint64_t>();
    return {1, failure};
  });
}

void readParsedStats(ModelStats& map, const std::filesystem::path& file_path) {
  readStatsImpl(map, file_path, [](const csv::CSVRow& row) -> Stats {
    const auto drive_day = row["drive_day"].get<uint64_t>();
    const auto failure = row["failure"].get<uint64_t>();
    return {drive_day, failure};
  });
}

void writeParsedStats(const ModelStats& map,
                      const filesystem::path& file_path,
                      bool enable_merge) {
  if (enable_merge && exists(file_path)) {
    ModelStats merged_map;
    readParsedStats(merged_map, file_path);
    mergeParsedStats(merged_map, map);
    return writeParsedStats(merged_map, file_path, false);
  }

  std::ofstream output{file_path};
  auto writer{csv::make_csv_writer_buffered(output)};

  writer << array<string_view, 3>{"model", "drive_day", "failure"};

  for (const auto& [model, stats] : map) {
    const auto& [drive_day, failure]{stats.counters};
    writer << make_tuple(model, drive_day, failure);
  }

  writer.flush();
}

void mergeParsedStats(ModelStats& map, const ModelStats& other) {
  for (const auto& [model, stats] : other) {
    map[model].merge(stats);
  }
}
}  // namespace bb