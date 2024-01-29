cmake_minimum_required (VERSION 3.19)
cmake_policy(SET CMP0091 NEW)	# Для CMAKE_MSVC_RUNTIME_LIBRARY
project ("Backblaze")
 
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/exe")

set(CMAKE_MSVC_RUNTIME_LIBRARY 
		"MultiThreaded$<$<CONFIG:Debug>:Debug>")	# static MSVC runtime (/MT)

set(Boost_USE_STATIC_LIBS ON)

find_package(Boost REQUIRED COMPONENTS)
find_package(fmt CONFIG REQUIRED)

add_subdirectory(rapidcsv)
add_subdirectory(unordered_dense)

add_executable (Backblaze
		"backblaze.hpp"
		"backblaze.cpp")

target_compile_features(Backblaze PRIVATE cxx_std_20)

target_link_libraries(Backblaze PRIVATE
		Boost::boost
		fmt::fmt-header-only
		rapidcsv
		unordered_dense::unordered_dense)
