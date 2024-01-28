cmake_minimum_required (VERSION 3.19)
cmake_policy(SET CMP0091 NEW)	# Для CMAKE_MSVC_RUNTIME_LIBRARY
project ("Backblaze")
 
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/exe")

set(CMAKE_MSVC_RUNTIME_LIBRARY 
		"MultiThreaded$<$<CONFIG:Debug>:Debug>")	# static MSVC runtime (/MT)

set(Boost_USE_STATIC_LIBS ON)

find_package(Boost REQUIRED COMPONENTS)
find_package(fmt CONFIG REQUIRED)

add_subdirectory(csv-parser)
add_subdirectory(unordered_dense)
	
set(BASIC_COMPILE_OPTIONS
		/MP  # Multiprocessor compilation
		/W4)  # Warnings level

set(RELEASE_COMPILE_OPTIONS
		/O2	    # Max optimization level
		/Ob3	# Aggressive inlining
		/Oi     # Enable intrinsics
		/Ot     # Prefer fast code
		/Oy)	# Omit frame pointers

set(LTO_COMPILE_OPTIONS
		/Gy		# Using COMDAT folding for functions
		/Gw		# Using COMDAT folding for variables
		/GL)		# Enable link time code generation


set(RELEASE_LINK_OPTIONS
		/OPT:REF)		# Eliminate unused code

set(LTO_LINK_OPTIONS
		/OPT:ICF		# Enable COMDAT folding
		/LTCG:INCREMENTAL)	# Force fast link timecode generation

add_executable (Backblaze
		"backblaze.hpp"
		"backblaze.cpp")

target_compile_definitions(Backblaze PRIVATE _USE_MATH_DEFINES)

target_compile_options(Backblaze PRIVATE
		${BASIC_COMPILE_OPTIONS}
		$<$<CONFIG:Release>:${RELEASE_COMPILE_OPTIONS}>
		$<$<CONFIG:Release>:${LTO_COMPILE_OPTIONS}>)

target_link_options(Backblaze PRIVATE
		${BASIC_LINK_OPTIONS}
		$<$<CONFIG:Release>:${RELEASE_LINK_OPTIONS}>
		$<$<CONFIG:Release>:${LTO_LINK_OPTIONS}>)

target_link_libraries(Backblaze PRIVATE
		Boost::boost
		csv
		fmt::fmt-header-only
		unordered_dense::unordered_dense)
